// Copyright 2013 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

//go:build aix || darwin || dragonfly || freebsd || (js && wasm) || linux || netbsd || openbsd || solaris || windows
// +build aix darwin dragonfly freebsd js,wasm linux netbsd openbsd solaris windows

package runtime

import (
	"runtime/internal/atomic"
	"unsafe"
)

// Integrated network poller (platform-independent part).
// A particular implementation (epoll/kqueue/port/AIX/Windows)
// must define the following functions:
//
// func netpollinit()
//     Initialize the poller. Only called once.
//
// func netpollopen(fd uintptr, pd *pollDesc) int32
//     Arm edge-triggered notifications for fd. The pd argument is to pass
//     back to netpollready when fd is ready. Return an errno value.
//
// func netpoll(delta int64) gList
//     Poll the network. If delta < 0, block indefinitely. If delta == 0,
//     poll without blocking. If delta > 0, block for up to delta nanoseconds.
//     Return a list of goroutines built by calling netpollready.
//
// func netpollBreak()
//     Wake up the network poller, assumed to be blocked in netpoll.
//
// func netpollIsPollDescriptor(fd uintptr) bool
//     Reports whether fd is a file descriptor used by the poller.

// Error codes returned by runtime_pollReset and runtime_pollWait.
// These must match the values in internal/poll/fd_poll_runtime.go.
const (
	pollNoError        = 0 // no error
	pollErrClosing     = 1 // descriptor is closed
	pollErrTimeout     = 2 // I/O timeout
	pollErrNotPollable = 3 // general error polling descriptor
)

// pollDesc contains 2 binary semaphores, rg and wg, to park reader and writer
// goroutines respectively. The semaphore can be in the following states:
// pdReady - io readiness notification is pending;
//           a goroutine consumes the notification by changing the state to nil.
// pdWait - a goroutine prepares to park on the semaphore, but not yet parked;
//          the goroutine commits to park by changing the state to G pointer,
//          or, alternatively, concurrent io notification changes the state to pdReady,
//          or, alternatively, concurrent timeout/close changes the state to nil.
// G pointer - the goroutine is blocked on the semaphore;
//             io notification or timeout/close changes the state to pdReady or nil respectively
//             and unparks the goroutine.
// nil - none of the above.
const (
	pdReady uintptr = 1
	pdWait  uintptr = 2
)

const pollBlockSize = 4 * 1024

// Network poller descriptor.
//
// No heap pointers.
//
//go:notinheap
type pollDesc struct {
	link *pollDesc // in pollcache, protected by pollcache.lock

	// The lock protects pollOpen, pollSetDeadline, pollUnblock and deadlineimpl operations.
	// This fully covers seq, rt and wt variables. fd is constant throughout the PollDesc lifetime.
	// pollReset, pollWait, pollWaitCanceled and runtime·netpollready (IO readiness notification)
	// proceed w/o taking the lock. So closing, everr, rg, rd, wg and wd are manipulated
	// in a lock-free way by all operations.
	// NOTE(dvyukov): the following code uses uintptr to store *g (rg/wg),
	// that will blow up when GC starts moving objects.
	lock    mutex // protects the following fields
	fd      uintptr
	closing bool
	everr   bool      // marks event scanning error happened
	user    uint32    // user settable cookie
	rseq    uintptr   // protects from stale read timers
	rg      uintptr   // pdReady, pdWait, G waiting for read or nil
	rt      timer     // read deadline timer (set if rt.f != nil)
	rd      int64     // read deadline
	wseq    uintptr   // protects from stale write timers
	wg      uintptr   // pdReady, pdWait, G waiting for write or nil
	wt      timer     // write deadline timer
	wd      int64     // write deadline
	self    *pollDesc // storage for indirect interface. See (*pollDesc).makeArg.
}

type pollCache struct {
	lock  mutex
	first *pollDesc
	// PollDesc objects must be type-stable,
	// because we can get ready notification from epoll/kqueue
	// after the descriptor is closed/reused.
	// Stale notifications are detected using seq variable,
	// seq is incremented when deadlines are changed or descriptor is reused.
}

var (
	netpollInitLock mutex
	netpollInited   uint32

	pollcache      pollCache
	netpollWaiters uint32
)

//go:linkname poll_runtime_pollServerInit internal/poll.runtime_pollServerInit
func poll_runtime_pollServerInit() {
	netpollGenericInit()
}

func netpollGenericInit() {
	if atomic.Load(&netpollInited) == 0 {
		lockInit(&netpollInitLock, lockRankNetpollInit)
		lock(&netpollInitLock)
		if netpollInited == 0 {
			netpollinit()
			atomic.Store(&netpollInited, 1)
		}
		unlock(&netpollInitLock)
	}
}

func netpollinited() bool {
	return atomic.Load(&netpollInited) != 0
}

//go:linkname poll_runtime_isPollServerDescriptor internal/poll.runtime_isPollServerDescriptor

// poll_runtime_isPollServerDescriptor reports whether fd is a
// descriptor being used by netpoll.
func poll_runtime_isPollServerDescriptor(fd uintptr) bool {
	return netpollIsPollDescriptor(fd)
}

//go:linkname poll_runtime_pollOpen internal/poll.runtime_pollOpen
func poll_runtime_pollOpen(fd uintptr) (*pollDesc, int) {
	pd := pollcache.alloc()
	lock(&pd.lock)
	if pd.wg != 0 && pd.wg != pdReady {
		throw("runtime: blocked write on free polldesc")
	}
	if pd.rg != 0 && pd.rg != pdReady {
		throw("runtime: blocked read on free polldesc")
	}
	pd.fd = fd
	pd.closing = false
	pd.everr = false
	pd.rseq++
	pd.rg = 0
	pd.rd = 0
	pd.wseq++
	pd.wg = 0
	pd.wd = 0
	pd.self = pd
	unlock(&pd.lock)

	var errno int32
	errno = netpollopen(fd, pd)
	return pd, int(errno)
}

//go:linkname poll_runtime_pollClose internal/poll.runtime_pollClose
func poll_runtime_pollClose(pd *pollDesc) {
	if !pd.closing {
		throw("runtime: close polldesc w/o unblock")
	}
	if pd.wg != 0 && pd.wg != pdReady {
		throw("runtime: blocked write on closing polldesc")
	}
	if pd.rg != 0 && pd.rg != pdReady {
		throw("runtime: blocked read on closing polldesc")
	}
	netpollclose(pd.fd)
	pollcache.free(pd)
}

func (c *pollCache) free(pd *pollDesc) {
	lock(&c.lock)
	pd.link = c.first
	c.first = pd
	unlock(&c.lock)
}

// poll_runtime_pollReset, which is internal/poll.runtime_pollReset,
// prepares a descriptor for polling in mode, which is 'r' or 'w'.
// This returns an error code; the codes are defined above.
//go:linkname poll_runtime_pollReset internal/poll.runtime_pollReset
func poll_runtime_pollReset(pd *pollDesc, mode int) int {
	errcode := netpollcheckerr(pd, int32(mode))
	if errcode != pollNoError {
		return errcode
	}
	if mode == 'r' {
		pd.rg = 0
	} else if mode == 'w' {
		pd.wg = 0
	}
	return pollNoError
}

// poll_runtime_pollWait, which is internal/poll.runtime_pollWait,
// waits for a descriptor to be ready for reading or writing,
// according to mode, which is 'r' or 'w'.
// This returns an error code; the codes are defined above.
//go:linkname poll_runtime_pollWait internal/poll.runtime_pollWait
func poll_runtime_pollWait(pd *pollDesc, mode int) int {
	errcode := netpollcheckerr(pd, int32(mode))
	if errcode != pollNoError {
		return errcode
	}
	// As for now only Solaris, illumos, and AIX use level-triggered IO.
	if GOOS == "solaris" || GOOS == "illumos" || GOOS == "aix" {
		netpollarm(pd, mode)
	}
	for !netpollblock(pd, int32(mode), false) {
		errcode = netpollcheckerr(pd, int32(mode))
		if errcode != pollNoError {
			return errcode
		}
		// Can happen if timeout has fired and unblocked us,
		// but before we had a chance to run, timeout has been reset.
		// Pretend it has not happened and retry.
	}
	return pollNoError
}

//go:linkname poll_runtime_pollWaitCanceled internal/poll.runtime_pollWaitCanceled
func poll_runtime_pollWaitCanceled(pd *pollDesc, mode int) {
	// This function is used only on windows after a failed attempt to cancel
	// a pending async IO operation. Wait for ioready, ignore closing or timeouts.
	for !netpollblock(pd, int32(mode), true) {
	}
}

// rdma modified
//go:linkname poll_runtime_pollGetDeadline internal/poll.runtime_pollGetDeadline
func poll_runtime_pollGetDeadline(pd *pollDesc, mode int) int64 {

	if mode == 'r' {
		return pd.rd
	}
	if mode == 'w' {
		return pd.wd
	}
	return 0
}

// rdma modified
//go:linkname poll_runtime_netpollcheckerr internal/poll.runtime_netpollcheckerr
func poll_runtime_netpollcheckerr(pd *pollDesc, mode int) int {
	errcode := netpollcheckerr(pd, int32(mode))
	if errcode != pollNoError {
		return errcode
	}
	return pollNoError
}

// rdma modified
//go:linkname poll_runtime_netpollmarkerr internal/poll.runtime_netpollmarkerr
func poll_runtime_netpollmarkerr(pd *pollDesc) {
	pd.everr = true
}

//go:linkname poll_runtime_pollSetDeadline internal/poll.runtime_pollSetDeadline
func poll_runtime_pollSetDeadline(pd *pollDesc, d int64, mode int) {
	lock(&pd.lock)
	if pd.closing {
		unlock(&pd.lock)
		return
	}
	rd0, wd0 := pd.rd, pd.wd
	combo0 := rd0 > 0 && rd0 == wd0
	if d > 0 {
		d += nanotime()
		if d <= 0 {
			// If the user has a deadline in the future, but the delay calculation
			// overflows, then set the deadline to the maximum possible value.
			d = 1<<63 - 1
		}
	}
	if mode == 'r' || mode == 'r'+'w' {
		pd.rd = d
	}
	if mode == 'w' || mode == 'r'+'w' {
		pd.wd = d
	}
	combo := pd.rd > 0 && pd.rd == pd.wd
	rtf := netpollReadDeadline
	if combo {
		rtf = netpollDeadline
	}
	if pd.rt.f == nil {
		if pd.rd > 0 {
			pd.rt.f = rtf
			// Copy current seq into the timer arg.
			// Timer func will check the seq against current descriptor seq,
			// if they differ the descriptor was reused or timers were reset.
			pd.rt.arg = pd.makeArg()
			pd.rt.seq = pd.rseq
			resettimer(&pd.rt, pd.rd)
		}
	} else if pd.rd != rd0 || combo != combo0 {
		pd.rseq++ // invalidate current timers
		if pd.rd > 0 {
			modtimer(&pd.rt, pd.rd, 0, rtf, pd.makeArg(), pd.rseq)
		} else {
			deltimer(&pd.rt)
			pd.rt.f = nil
		}
	}
	if pd.wt.f == nil {
		if pd.wd > 0 && !combo {
			pd.wt.f = netpollWriteDeadline
			pd.wt.arg = pd.makeArg()
			pd.wt.seq = pd.wseq
			resettimer(&pd.wt, pd.wd)
		}
	} else if pd.wd != wd0 || combo != combo0 {
		pd.wseq++ // invalidate current timers
		if pd.wd > 0 && !combo {
			modtimer(&pd.wt, pd.wd, 0, netpollWriteDeadline, pd.makeArg(), pd.wseq)
		} else {
			deltimer(&pd.wt)
			pd.wt.f = nil
		}
	}
	// If we set the new deadline in the past, unblock currently pending IO if any.
	var rg, wg *g
	if pd.rd < 0 || pd.wd < 0 {
		atomic.StorepNoWB(noescape(unsafe.Pointer(&wg)), nil) // full memory barrier between stores to rd/wd and load of rg/wg in netpollunblock
		if pd.rd < 0 {
			rg = netpollunblock(pd, 'r', false)
		}
		if pd.wd < 0 {
			wg = netpollunblock(pd, 'w', false)
		}
	}
	unlock(&pd.lock)
	if rg != nil {
		netpollgoready(rg, 3)
	}
	if wg != nil {
		netpollgoready(wg, 3)
	}
}

//go:linkname poll_runtime_pollUnblock internal/poll.runtime_pollUnblock
func poll_runtime_pollUnblock(pd *pollDesc) {
	lock(&pd.lock)
	if pd.closing {
		throw("runtime: unblock on closing polldesc")
	}
	pd.closing = true
	pd.rseq++
	pd.wseq++
	var rg, wg *g
	atomic.StorepNoWB(noescape(unsafe.Pointer(&rg)), nil) // full memory barrier between store to closing and read of rg/wg in netpollunblock
	// 若 pd 的 read/write 信号量为 pdWait ，则将信号量改为 0 并得到 waiting 状态的 goroutine
	// 若 pd 的 read/write 信号量为 pdReady ，则返回 nil
	// 若 pd 的 read/write 信号量为 0 ，则返回 nil
	rg = netpollunblock(pd, 'r', false)
	wg = netpollunblock(pd, 'w', false)
	if pd.rt.f != nil {
		deltimer(&pd.rt)
		pd.rt.f = nil
	}
	if pd.wt.f != nil {
		deltimer(&pd.wt)
		pd.wt.f = nil
	}
	unlock(&pd.lock)
	if rg != nil {
		netpollgoready(rg, 3)// 将此前处于 _Gwaiting 状态的 goroutine 改为 _Grunnable
	}
	if wg != nil {
		netpollgoready(wg, 3)// 将此前处于 _Gwaiting 状态的 goroutine 改为 _Grunnable
	}
}

// netpollready is called by the platform-specific netpoll function.
// It declares that the fd associated with pd is ready for I/O.
// The toRun argument is used to build a list of goroutines to return
// from netpoll. The mode argument is 'r', 'w', or 'r'+'w' to indicate
// whether the fd is ready for reading or writing or both.
//
// This may run while the world is stopped, so write barriers are not allowed.
//go:nowritebarrier
func netpollready(toRun *gList, pd *pollDesc, mode int32) {
	var rg, wg *g
	if mode == 'r' || mode == 'r'+'w' {
		rg = netpollunblock(pd, 'r', true)// 若 pd 的 read 信号量为 pdWait 或 0，则将信号量改为 pdReady 并得到waiting 状态的 goroutine
	}
	if mode == 'w' || mode == 'r'+'w' {
		wg = netpollunblock(pd, 'w', true)// 若 pd 的 write 信号量为 pdWait 或 0，则将信号量改为 pdReady 并得到waiting 状态的 goroutine
	}
	if rg != nil {
		toRun.push(rg)
	}
	if wg != nil {
		toRun.push(wg)
	}
}

func netpollcheckerr(pd *pollDesc, mode int32) int {
	if pd.closing {
		return pollErrClosing
	}
	if (mode == 'r' && pd.rd < 0) || (mode == 'w' && pd.wd < 0) {
		return pollErrTimeout
	}
	// Report an event scanning error only on a read event.
	// An error on a write event will be captured in a subsequent
	// write call that is able to report a more specific error.
	if mode == 'r' && pd.everr {
		return pollErrNotPollable
	}
	return pollNoError
}

func netpollblockcommit(gp *g, gpp unsafe.Pointer) bool {
	r := atomic.Casuintptr((*uintptr)(gpp), pdWait, uintptr(unsafe.Pointer(gp)))// 信号量若为 pdWait，更新为当前 gp
	if r {// 成功更新信号量（但不一定发生了变化，可能仍然是 pdWait）
		// Bump the count of goroutines waiting for the poller.
		// The scheduler uses this to decide whether to block
		// waiting for the poller if there is nothing else to do.
		atomic.Xadd(&netpollWaiters, 1)// 增加等待 poller 变为 ready 的goroutine 计数，提供给 调度器以便没有其他任务时停止对 poller 的等待
	}
	return r// 若 r 为 false，说明信号量已经不是 pdWait
}

func netpollgoready(gp *g, traceskip int) {
	atomic.Xadd(&netpollWaiters, -1)// netpollWaiters -1，减少当前 goroutine 栈中等待 poller 的计数
	goready(gp, traceskip+1)// 在 g0 上执行 ready 方法，// 将 gp goroutine 由 _Gwaiting 改为 _Grunnable
}

// returns true if IO is ready, or false if timedout or closed
// waitio - wait only for completed IO, ignore errors
func netpollblock(pd *pollDesc, mode int32, waitio bool) bool {
	// pollDesc 有两个信号量，分别表示尝试执行 read或write 操作的 goroutine 是否需要被park 住
	// 信号量的取值有： pdWait, pdReady, G waiting for read 或者 nil

	// 默认获得 rg 信号量指针保存到gpp；
	gpp := &pd.rg
	// 若传入 mod 为 w 表示要 goroutine 尝试执行 write 操作
	if mode == 'w' {
		gpp = &pd.wg
	}

	// set the gpp semaphore to pdWait
	// 将 gpp 指向的信号量值设置为 pdWait，表示暂时无法执行 read 或write，需要将 goroutine park等待 gpp 变为 pdReady
	for {
		old := *gpp
		if old == pdReady {// 若当前信号量已经是 pdReady，表示可以执行对应 read 或 write 操作，因此直接返回，无需对 goroutine 执行 park
			*gpp = 0 // 每次返回前都将信号量重置为 0
			return true
		}
		if old != 0 {// 若信号量不为0 ，表示已有goroutine 被park，处于等待 read/write 操作中
			throw("runtime: double wait")
		}
		if atomic.Casuintptr(gpp, 0, pdWait) {// 检查 gpp 指向的信号量是否为0，若为0 则将其更新为 pdWait 并返回true，完成信号量的设置
			break
		}
	}

	// need to recheck error states after setting gpp to pdWait
	// this is necessary because runtime_pollUnblock/runtime_pollSetDeadline/deadlineimpl
	// do the opposite: store to closing/rd/wd, membarrier, load of rg/wg
	if waitio || netpollcheckerr(pd, mode) == 0 {// 检查pd 是否超时 或无法 poll
		// 将当前 goroutine park，也称作“将当前 goroutine 设为 waiting 状态”
		// 并在系统栈中调用 netpollblockcommit ,直到 netpollblockcommit 返回false
		// 当 netpollblockcommit 返回false 时，goroutine 被恢复
		// netpollblockcommit 不应该访问当前 goroutine 的栈（G's stack），
		// 因为在调用 gopark() 之后，netpollblockcommit 可能被移动到其他 goroutine 上执行
		// 请注意：因为 netpollblockcommit 是在将 goroutine 设置为 waiting 状态后才被调用的
		// 当没有外部同步阻止 G 变为 ready 的情况下， netpollblockcommit 被调用时G 可能已经 ready了。
		// 如果 netpollblockcommit 返回 false，必须保证 G 无法被外部变为 erady
		// 
		// netpollblockcommit 返回false 时 ，信号量已经不是 pdWait
		gopark(netpollblockcommit, unsafe.Pointer(gpp), waitReasonIOWait, traceEvGoBlockNet, 5)
	}
	// be careful to not lose concurrent pdReady notification
	old := atomic.Xchguintptr(gpp, 0)// 将 信号量现有值保存到 old，并将信号量更新为 0
	if old > pdWait {// 若 netpollcheckerr!=0 或 gopark 恢复后信号量 >2，则信号量为非法值？
		throw("runtime: corrupted polldesc")
	}
	return old == pdReady// 判断 netpollcheckerr!=0 或 gopark 恢复后，信号量是否为 pdReady
}

// 若 pd 对应的 mode（read 或 write）信号量为 pdWait，则根据 ioready 更新信号量，并返回此前处于waiting 状态的 goroutine
// 若 pd 信号量已经是 pdReady，则返回nil
// 若 pd 信号量为 0，但 ioready 为 true，则将 pd 的信号量设为 pdReady，并返回 goroutine
func netpollunblock(pd *pollDesc, mode int32, ioready bool) *g {
	gpp := &pd.rg
	if mode == 'w' {
		gpp = &pd.wg
	}

	for {
		old := *gpp
		if old == pdReady {// 若当前信号量为 pdReady，返回 nil
			return nil
		}
		if old == 0 && !ioready {// 若当前信号量为0 ，且 ioready 为 false，则返回nil
			// Only set pdReady for ioready. runtime_pollWait
			// will check for timeout/cancel before waiting.
			return nil
		}
		var new uintptr
		if ioready {// 若入参 ioready 显示已经准备好执行 I/O 操作，则尝试将信号量改为 pdReady
			new = pdReady
		}
		if atomic.Casuintptr(gpp, old, new) {// 成功将信号量改为 pdReady
			if old == pdWait {// 重置值为 pdWait 的 old 信号量
				old = 0
			}
			return (*g)(unsafe.Pointer(old))// 返回 old 信号量对应的 goroutine？
		}
	}
}

func netpolldeadlineimpl(pd *pollDesc, seq uintptr, read, write bool) {
	lock(&pd.lock)
	// Seq arg is seq when the timer was set.
	// If it's stale, ignore the timer event.
	currentSeq := pd.rseq
	if !read {
		currentSeq = pd.wseq
	}
	if seq != currentSeq {
		// The descriptor was reused or timers were reset.
		unlock(&pd.lock)
		return
	}
	var rg *g
	if read {
		if pd.rd <= 0 || pd.rt.f == nil {
			throw("runtime: inconsistent read deadline")
		}
		pd.rd = -1
		atomic.StorepNoWB(unsafe.Pointer(&pd.rt.f), nil) // full memory barrier between store to rd and load of rg in netpollunblock
		rg = netpollunblock(pd, 'r', false)
	}
	var wg *g
	if write {
		if pd.wd <= 0 || pd.wt.f == nil && !read {
			throw("runtime: inconsistent write deadline")
		}
		pd.wd = -1
		atomic.StorepNoWB(unsafe.Pointer(&pd.wt.f), nil) // full memory barrier between store to wd and load of wg in netpollunblock
		wg = netpollunblock(pd, 'w', false)
	}
	unlock(&pd.lock)
	if rg != nil {
		netpollgoready(rg, 0)
	}
	if wg != nil {
		netpollgoready(wg, 0)
	}
}

func netpollDeadline(arg interface{}, seq uintptr) {
	netpolldeadlineimpl(arg.(*pollDesc), seq, true, true)
}

func netpollReadDeadline(arg interface{}, seq uintptr) {
	netpolldeadlineimpl(arg.(*pollDesc), seq, true, false)
}

func netpollWriteDeadline(arg interface{}, seq uintptr) {
	netpolldeadlineimpl(arg.(*pollDesc), seq, false, true)
}

func (c *pollCache) alloc() *pollDesc {
	lock(&c.lock)
	if c.first == nil {
		const pdSize = unsafe.Sizeof(pollDesc{})
		n := pollBlockSize / pdSize
		if n == 0 {
			n = 1
		}
		// Must be in non-GC memory because can be referenced
		// only from epoll/kqueue internals.
		mem := persistentalloc(n*pdSize, 0, &memstats.other_sys)
		for i := uintptr(0); i < n; i++ {
			pd := (*pollDesc)(add(mem, i*pdSize))
			pd.link = c.first
			c.first = pd
		}
	}
	pd := c.first
	c.first = pd.link
	lockInit(&pd.lock, lockRankPollDesc)
	unlock(&c.lock)
	return pd
}

// makeArg converts pd to an interface{}.
// makeArg does not do any allocation. Normally, such
// a conversion requires an allocation because pointers to
// go:notinheap types (which pollDesc is) must be stored
// in interfaces indirectly. See issue 42076.
func (pd *pollDesc) makeArg() (i interface{}) {
	x := (*eface)(unsafe.Pointer(&i))
	x._type = pdType
	x.data = unsafe.Pointer(&pd.self)
	return
}

var (
	pdEface interface{} = (*pollDesc)(nil)
	pdType  *_type      = efaceOf(&pdEface)._type
)
