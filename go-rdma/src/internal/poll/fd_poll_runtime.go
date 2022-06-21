// Copyright 2013 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

//go:build aix || darwin || dragonfly || freebsd || linux || netbsd || openbsd || windows || solaris
// +build aix darwin dragonfly freebsd linux netbsd openbsd windows solaris

package poll

import (
	"errors"
	"sync"
	"syscall"
	"time"
	_ "unsafe" // for go:linkname
)

// runtimeNano returns the current value of the runtime clock in nanoseconds.
//go:linkname runtimeNano runtime.nanotime
func runtimeNano() int64

func runtime_pollServerInit()
func runtime_pollOpen(fd uintptr) (uintptr, int)
func runtime_pollClose(ctx uintptr)
func runtime_pollWait(ctx uintptr, mode int) int
func runtime_pollWaitCanceled(ctx uintptr, mode int) int
func runtime_pollReset(ctx uintptr, mode int) int
func runtime_pollSetDeadline(ctx uintptr, d int64, mode int)
func runtime_pollUnblock(ctx uintptr)
func runtime_isPollServerDescriptor(fd uintptr) bool
func runtime_pollGetDeadline(ctx uintptr, mode int) int64
func runtime_netpollcheckerr(ctx uintptr, mode int) int
func runtime_netpollmarkerr(ctx uintptr)

type pollDesc struct {
	runtimeCtx uintptr
}

var serverInit sync.Once

func (pd *pollDesc) init(fd *FD) error {
	serverInit.Do(runtime_pollServerInit)// 单例模式执行 epollcreate，然后创建一个 pipe 并将read fd 加入 epoll 监听 EPOLLIN 事件
	// 初始化 pd的 rg 信号量等信息 
	// 并执行 epollctl(epfd,_EPOLL_CTL_ADD,fd,&ev) 将 fd 加入到 唯一 epoll 的监听队列中
	// 返回 pd 的指针赋值给 runtimeCtx
	ctx, errno := runtime_pollOpen(uintptr(fd.Sysfd))
	if errno != 0 {
		if ctx != 0 {
			runtime_pollUnblock(ctx)
			runtime_pollClose(ctx)
		}
		return errnoErr(syscall.Errno(errno))
	}
	// pd 可以理解为 epoll 中自定义的数据结构的指针；当 epoll_wait 返回时，fd 对应的 pd 会一同被返回，
	// 通过 pd 可以得知I/O 操作信息（比如 控制 goroutine 调度运行的信号量 rg/wg）
	// ctx 为一个 pd 指针，通过 sysAlloc 被分配了内存地址，并保存了 fd\rg 等自定义信息，
	// 并作为 ev.data 位置的数据，随着 epoll_wait 返回时一并返回
	pd.runtimeCtx = ctx
	return nil
}

func (pd *pollDesc) close() {
	if pd.runtimeCtx == 0 {
		return
	}
	runtime_pollClose(pd.runtimeCtx)
	pd.runtimeCtx = 0
}

// Evict evicts fd from the pending list, unblocking any I/O running on fd.
func (pd *pollDesc) evict() {
	if pd.runtimeCtx == 0 {
		return
	}
	runtime_pollUnblock(pd.runtimeCtx)
}

func (pd *pollDesc) prepare(mode int, isFile bool) error {
	if pd.runtimeCtx == 0 {
		return nil
	}
	res := runtime_pollReset(pd.runtimeCtx, mode)
	return convertErr(res, isFile)
}

func (pd *pollDesc) prepareRead(isFile bool) error {
	return pd.prepare('r', isFile)
}

// rdma modified
func (fd *FD) PrepareRead(isFile bool) error {
	return fd.pd.prepare('r', isFile)
}

// rdma modified
func (fd *FD) PrepareWrite(isFile bool) error {
	return fd.pd.prepare('w', isFile)
}

// rdma modified
func (fd *FD) PollCheckerr(mode int) error {
	res := runtime_netpollcheckerr(fd.pd.runtimeCtx, mode)
	switch res {
	case pollNoError:
		// println("rdma debug: netpollcheckerr return pollNoError")
		return nil
	case pollErrTimeout:
		// println("rdma debug: netpollcheckerr return ErrDeadlineExceeded")
		return ErrDeadlineExceeded
	}
	// println("rdma debug: netpollcheckerr return res ", res)
	return nil
}

// rdma modified
func (fd *FD) PollMarkerr() error {
	runtime_netpollmarkerr(fd.pd.runtimeCtx)
	return nil
}

func (pd *pollDesc) prepareWrite(isFile bool) error {
	return pd.prepare('w', isFile)
}

func (pd *pollDesc) wait(mode int, isFile bool) error {
	if pd.runtimeCtx == 0 {
		return errors.New("waiting for unsupported file type")
	}
	res := runtime_pollWait(pd.runtimeCtx, mode)
	return convertErr(res, isFile)
}

func (pd *pollDesc) waitRead(isFile bool) error {
	return pd.wait('r', isFile)
}

func (pd *pollDesc) waitWrite(isFile bool) error {
	return pd.wait('w', isFile)
}

func (pd *pollDesc) waitCanceled(mode int) {
	if pd.runtimeCtx == 0 {
		return
	}
	runtime_pollWaitCanceled(pd.runtimeCtx, mode)
}

func (pd *pollDesc) pollable() bool {
	return pd.runtimeCtx != 0
}

// rdma modified
func (fd *FD) Pollable() bool {
	return fd.pd.runtimeCtx != 0
}

// Error values returned by runtime_pollReset and runtime_pollWait.
// These must match the values in runtime/netpoll.go.
const (
	pollNoError        = 0
	pollErrClosing     = 1
	pollErrTimeout     = 2
	pollErrNotPollable = 3
)

func convertErr(res int, isFile bool) error {
	switch res {
	case pollNoError:
		return nil
	case pollErrClosing:
		return errClosing(isFile)
	case pollErrTimeout:
		return ErrDeadlineExceeded
	case pollErrNotPollable:
		return ErrNotPollable
	}
	println("unreachable: ", res)
	panic("unreachable")
}

// SetDeadline sets the read and write deadlines associated with fd.
func (fd *FD) SetDeadline(t time.Time) error {
	return setDeadlineImpl(fd, t, 'r'+'w')
}

// SetReadDeadline sets the read deadline associated with fd.
func (fd *FD) SetReadDeadline(t time.Time) error {
	return setDeadlineImpl(fd, t, 'r')
}

// SetWriteDeadline sets the write deadline associated with fd.
func (fd *FD) SetWriteDeadline(t time.Time) error {
	return setDeadlineImpl(fd, t, 'w')
}

// rdma modified
func (fd *FD) GotReadDeadline() bool {
	return gotDeadlineImpl(fd, 'r')
}

// rdma modified
func (fd *FD) GotWriteDeadline() bool {
	return gotDeadlineImpl(fd, 'w')
}

func setDeadlineImpl(fd *FD, t time.Time, mode int) error {
	var d int64
	if !t.IsZero() {
		d = int64(time.Until(t))
		if d == 0 {
			d = -1 // don't confuse deadline right now with no deadline
		}
	}
	if err := fd.incref(); err != nil {
		return err
	}
	defer fd.decref()
	if fd.pd.runtimeCtx == 0 {
		return ErrNoDeadline
	}
	runtime_pollSetDeadline(fd.pd.runtimeCtx, d, mode)
	return nil
}

// rdma modified
func gotDeadlineImpl(fd *FD, mode int) bool {
	if runtime_pollGetDeadline(fd.pd.runtimeCtx, mode) != 0 {
		return false
	}
	return true // reach deadline
}

// IsPollDescriptor reports whether fd is the descriptor being used by the poller.
// This is only used for testing.
func IsPollDescriptor(fd uintptr) bool {
	return runtime_isPollServerDescriptor(fd)
}
