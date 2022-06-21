// Copyright 2009 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

/*
Package net provides a portable interface for network I/O, including
TCP/IP, UDP, domain name resolution, and Unix domain sockets.

Although the package provides access to low-level networking
primitives, most clients will need only the basic interface provided
by the Dial, Listen, and Accept functions and the associated
Conn and Listener interfaces. The crypto/tls package uses
the same interfaces and similar Dial and Listen functions.

The Dial function connects to a server:

	conn, err := net.Dial("tcp", "golang.org:80")
	if err != nil {
		// handle error
	}
	fmt.Fprintf(conn, "GET / HTTP/1.0\r\n\r\n")
	status, err := bufio.NewReader(conn).ReadString('\n')
	// ...

The Listen function creates servers:

	ln, err := net.Listen("tcp", ":8080")
	if err != nil {
		// handle error
	}
	for {
		conn, err := ln.Accept()
		if err != nil {
			// handle error
		}
		go handleConnection(conn)
	}

Name Resolution

The method for resolving domain names, whether indirectly with functions like Dial
or directly with functions like LookupHost and LookupAddr, varies by operating system.

On Unix systems, the resolver has two options for resolving names.
It can use a pure Go resolver that sends DNS requests directly to the servers
listed in /etc/resolv.conf, or it can use a cgo-based resolver that calls C
library routines such as getaddrinfo and getnameinfo.

By default the pure Go resolver is used, because a blocked DNS request consumes
only a goroutine, while a blocked C call consumes an operating system thread.
When cgo is available, the cgo-based resolver is used instead under a variety of
conditions: on systems that do not let programs make direct DNS requests (OS X),
when the LOCALDOMAIN environment variable is present (even if empty),
when the RES_OPTIONS or HOSTALIASES environment variable is non-empty,
when the ASR_CONFIG environment variable is non-empty (OpenBSD only),
when /etc/resolv.conf or /etc/nsswitch.conf specify the use of features that the
Go resolver does not implement, and when the name being looked up ends in .local
or is an mDNS name.

The resolver decision can be overridden by setting the netdns value of the
GODEBUG environment variable (see package runtime) to go or cgo, as in:

	export GODEBUG=netdns=go    # force pure Go resolver
	export GODEBUG=netdns=cgo   # force cgo resolver

The decision can also be forced while building the Go source tree
by setting the netgo or netcgo build tag.

A numeric netdns setting, as in GODEBUG=netdns=1, causes the resolver
to print debugging information about its decisions.
To force a particular resolver while also printing debugging information,
join the two settings by a plus sign, as in GODEBUG=netdns=go+1.

On Plan 9, the resolver always accesses /net/cs and /net/dns.

On Windows, the resolver always uses C library functions, such as GetAddrInfo and DnsQuery.

*/
package net

import (
	"context"
	"errors"
	"internal/poll"
	"io"
	"os"
	"rdma"
	"runtime"
	"sync"
	"syscall"
	"time"
	// "log"
)

// netGo and netCgo contain the state of the build tags used
// to build this binary, and whether cgo is available.
// conf.go mirrors these into conf for easier testing.
var (
	netGo  bool // set true in cgo_stub.go for build tag "netgo" (or no cgo)
	netCgo bool // set true in conf_netcgo.go for build tag "netcgo"
)

const (
	maxRW = 1 << 30 // rdma modified
)

// Addr represents a network end point address.
//
// The two methods Network and String conventionally return strings
// that can be passed as the arguments to Dial, but the exact form
// and meaning of the strings is up to the implementation.
type Addr interface {
	Network() string // name of the network (for example, "tcp", "udp")
	String() string  // string form of address (for example, "192.0.2.1:25", "[2001:db8::1]:80")
}

// Conn is a generic stream-oriented network connection.
//
// Multiple goroutines may invoke methods on a Conn simultaneously.
type Conn interface {
	// Read reads data from the connection.
	// Read can be made to time out and return an error after a fixed
	// time limit; see SetDeadline and SetReadDeadline.
	Read(b []byte) (n int, err error)

	// Write writes data to the connection.
	// Write can be made to time out and return an error after a fixed
	// time limit; see SetDeadline and SetWriteDeadline.
	Write(b []byte) (n int, err error)

	// Close closes the connection.
	// Any blocked Read or Write operations will be unblocked and return errors.
	Close() error

	// LocalAddr returns the local network address.
	LocalAddr() Addr

	// RemoteAddr returns the remote network address.
	RemoteAddr() Addr

	// SetDeadline sets the read and write deadlines associated
	// with the connection. It is equivalent to calling both
	// SetReadDeadline and SetWriteDeadline.
	//
	// A deadline is an absolute time after which I/O operations
	// fail instead of blocking. The deadline applies to all future
	// and pending I/O, not just the immediately following call to
	// Read or Write. After a deadline has been exceeded, the
	// connection can be refreshed by setting a deadline in the future.
	//
	// If the deadline is exceeded a call to Read or Write or to other
	// I/O methods will return an error that wraps os.ErrDeadlineExceeded.
	// This can be tested using errors.Is(err, os.ErrDeadlineExceeded).
	// The error's Timeout method will return true, but note that there
	// are other possible errors for which the Timeout method will
	// return true even if the deadline has not been exceeded.
	//
	// An idle timeout can be implemented by repeatedly extending
	// the deadline after successful Read or Write calls.
	//
	// A zero value for t means I/O operations will not time out.
	SetDeadline(t time.Time) error

	// SetReadDeadline sets the deadline for future Read calls
	// and any currently-blocked Read call.
	// A zero value for t means Read will not time out.
	SetReadDeadline(t time.Time) error

	// SetWriteDeadline sets the deadline for future Write calls
	// and any currently-blocked Write call.
	// Even if write times out, it may return n > 0, indicating that
	// some of the data was successfully written.
	// A zero value for t means Write will not time out.
	SetWriteDeadline(t time.Time) error
}

type conn struct {
	fd *netFD
}

func (c *conn) ok() bool { return c != nil && c.fd != nil }

// Implementation of the Conn interface.

// rdma modified
func ignoringEINTRIO(fn func(fd int, p []byte) (int, error), fd int, p []byte) (int, error) {
	for {
		n, err := fn(fd, p)
		if err != syscall.EINTR {
			return n, err
		}
	}
}

func fdRead(cfd *netFD, p []byte) (int, error) {
	// 增加fd的引用计数
	if err := cfd.ReadLock(); err != nil {
		return 0, err
	}
	// fmt.Printf("rdma read: fdRead:%v got ReadLock\n",cfd.pfd.Sysfd)
	//与accept类似，此处通常仅用于减少fd的引用计数，在read失败后需要手动close连接
	defer func() {
		cfd.ReadUnlock()
	}()
	if len(p) == 0 {
		return 0, nil
	}
	// 源码 prepareRead 完成的工作：
	// 1. 检查 pd.runtimeCtx 是否还存在（即 尚未执行 src/internal/poll/fd_poll_runtime.go: func (pd *pollDesc) close()）
	// 2. 执行 src/time/netpoll.go: func poll_runtime_pollReset 检查 netpollcheckerr，并将 pd 的 rg/wg 信号量重置为 0
	// rdma 改造后 不再使用 netpoll 中创建的 epoll
	// 但此处只是修改几个变量，不执行实际 epoll_wait 操作，因此可以保留
	// 但其中的 netpollcheckerr 检查是否 timeout或 不在pollable 的逻辑如何迁移到 rdma 的 poll 过程中？？
	// TODO(zhangpc)
	if err := cfd.pfd.PrepareRead(false); err != nil {
		return 0, err
	}

	// 因为 rdma/rsocket 模拟 tcp socket，替换的是 listenTCP 流程中 SOCK_STREAM 类型 socket
	// 因此 newFD 时 IsStream == syscall.SOCK_STREAM 条件仍满足，fd.IsStream == true
	if cfd.pfd.IsStream && len(p) > maxRW {
		p = p[:maxRW] // 每次执行rread 不超过1G
	}

	for {

		// 参考 ignoringEINTR 的注释，以及搜索 SA_RESTART，ingore EINTER 应该是较为通用的做法，暂时保留
		n, err := ignoringEINTRIO(rdma.RRead, cfd.pfd.Sysfd, p)

		rdma.DPrintf("rdma debug: ignoringEINTRRead(%d) n:%d err:%v\n", cfd.pfd.Sysfd, n, err)

		if err != nil {
			n = 0
			if err == syscall.EAGAIN && cfd.pfd.Pollable() { // rdma fixme: need check pd is destroy? like fd.pd.pollable()
				// 模拟waitRead() 逻辑：循环执行waitRead()，直到 waitRead() 返回非空err
				ret := 0
				re := 0
				// 由于fds 中的fd 为非阻塞，rpoll 会立即返回；
				// 使用循环判断返回值为0时 继续执行rpoll，直到rpoll 返回值不为0
				for ret == 0 && cfd.pfd.Pollable() && !cfd.pfd.IsDestroied {
					err = cfd.pfd.PollCheckerr('r')
					if err != nil {
						return 0, err
					}
					ret, re, err = rdma.WrappedRpoll(cfd.pfd.Sysfd, rdma.POLLIN|rdma.POLLHUP|rdma.POLLERR, 0)
				}
				if cfd.pfd.IsDestroied {
					return n, errors.New("use of closed network connection")
				}
				if !cfd.pfd.Pollable() {
					return n, errors.New(" rsocket not pollable")
				}
				// fmt.Printf("rdma debug: Read %d polled ret:%d n:%d err:%v\n", cfd.pfd.Sysfd, ret, re, err)
				if ret == 1 { // 若rpoll 返回值为1，表示成功poll 到了fd 上的事件
					// 判断poll 到的事件中是否存在 pollerr 或pollup，若存在则 r!=0 ，后续代码可以根据r！=0 判断链接不再具备继续read/write 的条件
					// 若revents 中不存在pollerr 或pollup，则后续代码根据r==0 判断fd 上已产生了时间且可以继续read/write
					ret = int(re & (rdma.POLLERR | rdma.POLLHUP)) // pollup 表示tcp 连接已经收到FIN 挥手，即对端已断开链接；pollerr 表示此fd 上后续io 会出错
				}
				if ret != 0 { // 链接已断开或出错，返回错误
					// fmt.Printf("rdma debug: Read %d polled ERR OR HUP ret:%d n:%d err:%v\n", cfd.pfd.Sysfd, ret, n, err)
					cfd.pfd.PollMarkerr() // mark poll err
					return n, err
				}
				continue // poll 到了事件且链接未断开或出错，立刻回到for 开始位置继续执行下一次read
			} else {
				return 0, err
			}
		}
		// 参考 "eofError"
		if n == 0 && err == nil { // && fd.ZeroReadIsEOF {
			err = io.EOF
		}
		return n, err
	}
}

// Read implements the Conn Read method.
func (c *conn) Read(b []byte) (n int, err error) {

	// // origin code
	// if !c.ok() {
	// 	return 0, syscall.EINVAL
	// }
	// n, err := c.fd.Read(b)
	// if err != nil && err != io.EOF {
	// 	err = &OpError{Op: "read", Net: c.fd.net, Source: c.fd.laddr, Addr: c.fd.raddr, Err: err}
	// }
	// return n, err

	// rdma modified
	if !c.ok() {
		return 0, syscall.EINVAL
	}

	// rdma.DPrintf("rdma read: fd(%v) start read\n",c.fd.pfd.Sysfd)

	n, err = fdRead(c.fd, b)

	// rdma.DPrintf("rdma read: fd(%v) done read,ret n:%v, err:%v\n",c.fd.pfd.Sysfd,n,err)

	// rdma TODO: need runtime.KeepAlive(fd)?
	runtime.KeepAlive(c.fd)
	if err != nil && err != io.EOF {
		err = &OpError{Op: "read", Net: c.fd.net, Source: c.fd.laddr, Addr: c.fd.raddr, Err: err}
	}

	return n, err
	// modify end

}

func fdWrite(cfd *netFD, p []byte) (nn int, err error) {
	if err := cfd.WriteLock(); err != nil {
		return 0, err
	}
	// fmt.Printf("rdma write: fdWrite:%v got WriteLock\n",cfd.pfd.Sysfd)
	defer func() {
		cfd.WriteUnlock()
	}()
	// 源码 prepareWrite 完成的工作与 PrepareRead 类似，只是 pollReset 时重置的信号量为 wg
	if err := cfd.pfd.PrepareWrite(false); err != nil {
		return 0, err
	}
	for {
		max := len(p)
		if cfd.pfd.IsStream && max-nn > maxRW { // maxRW 设为1G，表示rwrite 每次最大写入1GB 数据
			max = nn + maxRW // 将max 设为 max 与 maxRW-nn 两者较小值，保证max 不超过len(p)，也不超过maxRW
		}
		wn, err := ignoringEINTRIO(rdma.RWrite, cfd.pfd.Sysfd, p[nn:max]) // 尝试rwrite
		if wn > 0 {                                                       // 若rwrite 返回值为正，则增加已写数据记录
			nn += wn
		}
		if nn == len(p) { // 若已写数据达到buffer 最大长度，返回本次Write 结果
			return nn, err
		}
		//wn < 0 &&  //&& fd.pd.pollable() {// 若rwite 返回值为负，说明未能成功写数据；
		// 若未成功写入时errno 为EAGAIN，则执行rpoll
		if err == syscall.EAGAIN && cfd.pfd.Pollable() && !cfd.pfd.IsDestroied {
			ret := 0
			re := 0
			// 由于fds 中的fd 为非阻塞，rpoll 会立即返回；
			// 使用循环判断返回值为0时 继续执行rpoll，直到rpoll 返回值不为0
			for ret == 0 && cfd.pfd.Pollable() && !cfd.pfd.IsDestroied {
				err = cfd.pfd.PollCheckerr('w')
				if err != nil {
					return 0, err
					// break
				}
				ret, re, err = rdma.WrappedRpoll(cfd.pfd.Sysfd, rdma.POLLOUT, 0) // rdma fixme:
			}
			if cfd.pfd.IsDestroied {
				return nn, errors.New("use of closed network connection")
			}
			if !cfd.pfd.Pollable() {
				return nn, errors.New(" rsocket not pollable")
			}
			// 若rpoll 返回值为1，表示成功poll 到了fd 上的事件
			if ret == 1 {
				// 判断poll 到的事件中是否存在 pollerr 或pollup，若存在则 r!=0 ，后续代码可以根据r！=0 判断链接不再具备继续read/write 的条件
				// 若revents 中不存在pollerr 或pollup，则后续代码根据r==0 判断fd 上已产生了时间且可以继续read/write
				ret = int(re & (rdma.POLLERR | rdma.POLLHUP)) // pollup 表示tcp 连接已经收到FIN 挥手，即对端已断开链接；pollerr 表示此fd 上后续io 会出错
			}
			// 链接已断开或出错，返回错误
			if ret != 0 {
				// log.Printf("rdma debug: Write polled ERR OR HUP\n")
				return ret, err
			}
			continue // poll 到了事件且链接未断开或出错，立刻回到for 开始位置继续执行下一次rwite
		}

		rdma.DPrintf("rdma debug: rdma.RWrite return wn %v err %v \n", wn, err)

		return nn, err
	}
}

// Write implements the Conn Write method.
func (c *conn) Write(b []byte) (n int, err error) {

	// // origin code
	// if !c.ok() {
	// 	return 0, syscall.EINVAL
	// }
	// n, err := c.fd.Write(b)
	// if err != nil {
	// 	err = &OpError{Op: "write", Net: c.fd.net, Source: c.fd.laddr, Addr: c.fd.raddr, Err: err}
	// }
	// return n, err

	if !c.ok() {
		return 0, syscall.EINVAL
	}
	// // rdma modified using rpoll

	// rdma.DPrintf("rdma write: fd(%v) start write\n",c.fd.pfd.Sysfd)

	n, err = fdWrite(c.fd, b)

	// rdma.DPrintf("rdma write: fd(%v) done write,ret n:%v, err:%v\n",c.fd.pfd.Sysfd,n,err)

	// rdma TODO: need runtime.KeepAlive(fd)?
	// 或许可以移动到执行 writeUnlock 的 defer func 里？
	runtime.KeepAlive(c.fd)
	if err != nil {
		err = &OpError{Op: "write", Net: c.fd.net, Source: c.fd.laddr, Addr: c.fd.raddr, Err: err}
	}
	return n, err
}

// Close closes the connection.
func (c *conn) Close() error {
	if !c.ok() {
		return syscall.EINVAL
	}
	// sysfd := c.fd.pfd.Sysfd
	err := c.fd.Close()
	if err != nil {
		err = &OpError{Op: "close", Net: c.fd.net, Source: c.fd.laddr, Addr: c.fd.raddr, Err: err}
	}
	// if c.fd.pfd.IsDestroied {
	// 	rdma.RClose(sysfd)
	// }
	return err
}

// LocalAddr returns the local network address.
// The Addr returned is shared by all invocations of LocalAddr, so
// do not modify it.
func (c *conn) LocalAddr() Addr {
	if !c.ok() {
		return nil
	}
	return c.fd.laddr
}

// RemoteAddr returns the remote network address.
// The Addr returned is shared by all invocations of RemoteAddr, so
// do not modify it.
func (c *conn) RemoteAddr() Addr {
	if !c.ok() {
		return nil
	}
	return c.fd.raddr
}

// SetDeadline implements the Conn SetDeadline method.
func (c *conn) SetDeadline(t time.Time) error {
	if !c.ok() {
		return syscall.EINVAL
	}
	if err := c.fd.SetDeadline(t); err != nil {
		return &OpError{Op: "set", Net: c.fd.net, Source: nil, Addr: c.fd.laddr, Err: err}
	}
	return nil
}

// SetReadDeadline implements the Conn SetReadDeadline method.
func (c *conn) SetReadDeadline(t time.Time) error {
	if !c.ok() {
		return syscall.EINVAL
	}
	if err := c.fd.SetReadDeadline(t); err != nil {
		return &OpError{Op: "set", Net: c.fd.net, Source: nil, Addr: c.fd.laddr, Err: err}
	}
	return nil
}

// SetWriteDeadline implements the Conn SetWriteDeadline method.
func (c *conn) SetWriteDeadline(t time.Time) error {
	if !c.ok() {
		return syscall.EINVAL
	}
	if err := c.fd.SetWriteDeadline(t); err != nil {
		return &OpError{Op: "set", Net: c.fd.net, Source: nil, Addr: c.fd.laddr, Err: err}
	}
	return nil
}

// SetReadBuffer sets the size of the operating system's
// receive buffer associated with the connection.
func (c *conn) SetReadBuffer(bytes int) error {
	if !c.ok() {
		return syscall.EINVAL
	}
	if err := setReadBuffer(c.fd, bytes); err != nil {
		return &OpError{Op: "set", Net: c.fd.net, Source: nil, Addr: c.fd.laddr, Err: err}
	}
	return nil
}

// SetWriteBuffer sets the size of the operating system's
// transmit buffer associated with the connection.
func (c *conn) SetWriteBuffer(bytes int) error {
	if !c.ok() {
		return syscall.EINVAL
	}
	if err := setWriteBuffer(c.fd, bytes); err != nil {
		return &OpError{Op: "set", Net: c.fd.net, Source: nil, Addr: c.fd.laddr, Err: err}
	}
	return nil
}

// File returns a copy of the underlying os.File.
// It is the caller's responsibility to close f when finished.
// Closing c does not affect f, and closing f does not affect c.
//
// The returned os.File's file descriptor is different from the connection's.
// Attempting to change properties of the original using this duplicate
// may or may not have the desired effect.
func (c *conn) File() (f *os.File, err error) {
	f, err = c.fd.dup()
	if err != nil {
		err = &OpError{Op: "file", Net: c.fd.net, Source: c.fd.laddr, Addr: c.fd.raddr, Err: err}
	}
	return
}

// PacketConn is a generic packet-oriented network connection.
//
// Multiple goroutines may invoke methods on a PacketConn simultaneously.
type PacketConn interface {
	// ReadFrom reads a packet from the connection,
	// copying the payload into p. It returns the number of
	// bytes copied into p and the return address that
	// was on the packet.
	// It returns the number of bytes read (0 <= n <= len(p))
	// and any error encountered. Callers should always process
	// the n > 0 bytes returned before considering the error err.
	// ReadFrom can be made to time out and return an error after a
	// fixed time limit; see SetDeadline and SetReadDeadline.
	ReadFrom(p []byte) (n int, addr Addr, err error)

	// WriteTo writes a packet with payload p to addr.
	// WriteTo can be made to time out and return an Error after a
	// fixed time limit; see SetDeadline and SetWriteDeadline.
	// On packet-oriented connections, write timeouts are rare.
	WriteTo(p []byte, addr Addr) (n int, err error)

	// Close closes the connection.
	// Any blocked ReadFrom or WriteTo operations will be unblocked and return errors.
	Close() error

	// LocalAddr returns the local network address.
	LocalAddr() Addr

	// SetDeadline sets the read and write deadlines associated
	// with the connection. It is equivalent to calling both
	// SetReadDeadline and SetWriteDeadline.
	//
	// A deadline is an absolute time after which I/O operations
	// fail instead of blocking. The deadline applies to all future
	// and pending I/O, not just the immediately following call to
	// Read or Write. After a deadline has been exceeded, the
	// connection can be refreshed by setting a deadline in the future.
	//
	// If the deadline is exceeded a call to Read or Write or to other
	// I/O methods will return an error that wraps os.ErrDeadlineExceeded.
	// This can be tested using errors.Is(err, os.ErrDeadlineExceeded).
	// The error's Timeout method will return true, but note that there
	// are other possible errors for which the Timeout method will
	// return true even if the deadline has not been exceeded.
	//
	// An idle timeout can be implemented by repeatedly extending
	// the deadline after successful ReadFrom or WriteTo calls.
	//
	// A zero value for t means I/O operations will not time out.
	SetDeadline(t time.Time) error

	// SetReadDeadline sets the deadline for future ReadFrom calls
	// and any currently-blocked ReadFrom call.
	// A zero value for t means ReadFrom will not time out.
	SetReadDeadline(t time.Time) error

	// SetWriteDeadline sets the deadline for future WriteTo calls
	// and any currently-blocked WriteTo call.
	// Even if write times out, it may return n > 0, indicating that
	// some of the data was successfully written.
	// A zero value for t means WriteTo will not time out.
	SetWriteDeadline(t time.Time) error
}

var listenerBacklogCache struct {
	sync.Once
	val int
}

// listenerBacklog is a caching wrapper around maxListenerBacklog.
func listenerBacklog() int {
	listenerBacklogCache.Do(func() { listenerBacklogCache.val = maxListenerBacklog() })
	return listenerBacklogCache.val
}

// A Listener is a generic network listener for stream-oriented protocols.
//
// Multiple goroutines may invoke methods on a Listener simultaneously.
type Listener interface {
	// Accept waits for and returns the next connection to the listener.
	Accept() (Conn, error)

	// Close closes the listener.
	// Any blocked Accept operations will be unblocked and return errors.
	Close() error

	// Addr returns the listener's network address.
	Addr() Addr
}

// An Error represents a network error.
type Error interface {
	error
	Timeout() bool   // Is the error a timeout?
	Temporary() bool // Is the error temporary?
}

// Various errors contained in OpError.
var (
	// For connection setup operations.
	errNoSuitableAddress = errors.New("no suitable address found")

	// For connection setup and write operations.
	errMissingAddress = errors.New("missing address")

	// For both read and write operations.
	errCanceled         = errors.New("operation was canceled")
	ErrWriteToConnected = errors.New("use of WriteTo with pre-connected connection")
)

// mapErr maps from the context errors to the historical internal net
// error values.
//
// TODO(bradfitz): get rid of this after adjusting tests and making
// context.DeadlineExceeded implement net.Error?
func mapErr(err error) error {
	switch err {
	case context.Canceled:
		return errCanceled
	case context.DeadlineExceeded:
		return errTimeout
	default:
		return err
	}
}

// OpError is the error type usually returned by functions in the net
// package. It describes the operation, network type, and address of
// an error.
type OpError struct {
	// Op is the operation which caused the error, such as
	// "read" or "write".
	Op string

	// Net is the network type on which this error occurred,
	// such as "tcp" or "udp6".
	Net string

	// For operations involving a remote network connection, like
	// Dial, Read, or Write, Source is the corresponding local
	// network address.
	Source Addr

	// Addr is the network address for which this error occurred.
	// For local operations, like Listen or SetDeadline, Addr is
	// the address of the local endpoint being manipulated.
	// For operations involving a remote network connection, like
	// Dial, Read, or Write, Addr is the remote address of that
	// connection.
	Addr Addr

	// Err is the error that occurred during the operation.
	// The Error method panics if the error is nil.
	Err error
}

func (e *OpError) Unwrap() error { return e.Err }

func (e *OpError) Error() string {
	if e == nil {
		return "<nil>"
	}
	s := e.Op
	if e.Net != "" {
		s += " " + e.Net
	}
	if e.Source != nil {
		s += " " + e.Source.String()
	}
	if e.Addr != nil {
		if e.Source != nil {
			s += "->"
		} else {
			s += " "
		}
		s += e.Addr.String()
	}
	s += ": " + e.Err.Error()
	return s
}

var (
	// aLongTimeAgo is a non-zero time, far in the past, used for
	// immediate cancellation of dials.
	aLongTimeAgo = time.Unix(1, 0)

	// nonDeadline and noCancel are just zero values for
	// readability with functions taking too many parameters.
	noDeadline = time.Time{}
	noCancel   = (chan struct{})(nil)
)

type timeout interface {
	Timeout() bool
}

func (e *OpError) Timeout() bool {
	if ne, ok := e.Err.(*os.SyscallError); ok {
		t, ok := ne.Err.(timeout)
		return ok && t.Timeout()
	}
	t, ok := e.Err.(timeout)
	return ok && t.Timeout()
}

type temporary interface {
	Temporary() bool
}

func (e *OpError) Temporary() bool {
	// Treat ECONNRESET and ECONNABORTED as temporary errors when
	// they come from calling accept. See issue 6163.
	if e.Op == "accept" && isConnError(e.Err) {
		return true
	}

	if ne, ok := e.Err.(*os.SyscallError); ok {
		t, ok := ne.Err.(temporary)
		return ok && t.Temporary()
	}
	t, ok := e.Err.(temporary)
	return ok && t.Temporary()
}

// A ParseError is the error type of literal network address parsers.
type ParseError struct {
	// Type is the type of string that was expected, such as
	// "IP address", "CIDR address".
	Type string

	// Text is the malformed text string.
	Text string
}

func (e *ParseError) Error() string { return "invalid " + e.Type + ": " + e.Text }

type AddrError struct {
	Err  string
	Addr string
}

func (e *AddrError) Error() string {
	if e == nil {
		return "<nil>"
	}
	s := e.Err
	if e.Addr != "" {
		s = "address " + e.Addr + ": " + s
	}
	return s
}

func (e *AddrError) Timeout() bool   { return false }
func (e *AddrError) Temporary() bool { return false }

type UnknownNetworkError string

func (e UnknownNetworkError) Error() string   { return "unknown network " + string(e) }
func (e UnknownNetworkError) Timeout() bool   { return false }
func (e UnknownNetworkError) Temporary() bool { return false }

type InvalidAddrError string

func (e InvalidAddrError) Error() string   { return string(e) }
func (e InvalidAddrError) Timeout() bool   { return false }
func (e InvalidAddrError) Temporary() bool { return false }

// errTimeout exists to return the historical "i/o timeout" string
// for context.DeadlineExceeded. See mapErr.
// It is also used when Dialer.Deadline is exceeded.
//
// TODO(iant): We could consider changing this to os.ErrDeadlineExceeded
// in the future, but note that that would conflict with the TODO
// at mapErr that suggests changing it to context.DeadlineExceeded.
var errTimeout error = &timeoutError{}

type timeoutError struct{}

func (e *timeoutError) Error() string   { return "i/o timeout" }
func (e *timeoutError) Timeout() bool   { return true }
func (e *timeoutError) Temporary() bool { return true }

// DNSConfigError represents an error reading the machine's DNS configuration.
// (No longer used; kept for compatibility.)
type DNSConfigError struct {
	Err error
}

func (e *DNSConfigError) Unwrap() error   { return e.Err }
func (e *DNSConfigError) Error() string   { return "error reading DNS config: " + e.Err.Error() }
func (e *DNSConfigError) Timeout() bool   { return false }
func (e *DNSConfigError) Temporary() bool { return false }

// Various errors contained in DNSError.
var (
	errNoSuchHost = errors.New("no such host")
)

// DNSError represents a DNS lookup error.
type DNSError struct {
	Err         string // description of the error
	Name        string // name looked for
	Server      string // server used
	IsTimeout   bool   // if true, timed out; not all timeouts set this
	IsTemporary bool   // if true, error is temporary; not all errors set this
	IsNotFound  bool   // if true, host could not be found
}

func (e *DNSError) Error() string {
	if e == nil {
		return "<nil>"
	}
	s := "lookup " + e.Name
	if e.Server != "" {
		s += " on " + e.Server
	}
	s += ": " + e.Err
	return s
}

// Timeout reports whether the DNS lookup is known to have timed out.
// This is not always known; a DNS lookup may fail due to a timeout
// and return a DNSError for which Timeout returns false.
func (e *DNSError) Timeout() bool { return e.IsTimeout }

// Temporary reports whether the DNS error is known to be temporary.
// This is not always known; a DNS lookup may fail due to a temporary
// error and return a DNSError for which Temporary returns false.
func (e *DNSError) Temporary() bool { return e.IsTimeout || e.IsTemporary }

// errClosed exists just so that the docs for ErrClosed don't mention
// the internal package poll.
var errClosed = poll.ErrNetClosing

// ErrClosed is the error returned by an I/O call on a network
// connection that has already been closed, or that is closed by
// another goroutine before the I/O is completed. This may be wrapped
// in another error, and should normally be tested using
// errors.Is(err, net.ErrClosed).
var ErrClosed = errClosed

type writerOnly struct {
	io.Writer
}

// Fallback implementation of io.ReaderFrom's ReadFrom, when sendfile isn't
// applicable.
func genericReadFrom(w io.Writer, r io.Reader) (n int64, err error) {
	// Use wrapper to hide existing r.ReadFrom from io.Copy.
	return io.Copy(writerOnly{w}, r)
}

// Limit the number of concurrent cgo-using goroutines, because
// each will block an entire operating system thread. The usual culprit
// is resolving many DNS names in separate goroutines but the DNS
// server is not responding. Then the many lookups each use a different
// thread, and the system or the program runs out of threads.

var threadLimit chan struct{}

var threadOnce sync.Once

func acquireThread() {
	threadOnce.Do(func() {
		threadLimit = make(chan struct{}, concurrentThreadsLimit())
	})
	threadLimit <- struct{}{}
}

func releaseThread() {
	<-threadLimit
}

// buffersWriter is the interface implemented by Conns that support a
// "writev"-like batch write optimization.
// writeBuffers should fully consume and write all chunks from the
// provided Buffers, else it should report a non-nil error.
type buffersWriter interface {
	writeBuffers(*Buffers) (int64, error)
}

// Buffers contains zero or more runs of bytes to write.
//
// On certain machines, for certain types of connections, this is
// optimized into an OS-specific batch write operation (such as
// "writev").
type Buffers [][]byte

var (
	_ io.WriterTo = (*Buffers)(nil)
	_ io.Reader   = (*Buffers)(nil)
)

func (v *Buffers) WriteTo(w io.Writer) (n int64, err error) {
	if wv, ok := w.(buffersWriter); ok {
		return wv.writeBuffers(v)
	}
	for _, b := range *v {
		nb, err := w.Write(b)
		n += int64(nb)
		if err != nil {
			v.consume(n)
			return n, err
		}
	}
	v.consume(n)
	return n, nil
}

func (v *Buffers) Read(p []byte) (n int, err error) {
	for len(p) > 0 && len(*v) > 0 {
		n0 := copy(p, (*v)[0])
		v.consume(int64(n0))
		p = p[n0:]
		n += n0
	}
	if len(*v) == 0 {
		err = io.EOF
	}
	return
}

func (v *Buffers) consume(n int64) {
	for len(*v) > 0 {
		ln0 := int64(len((*v)[0]))
		if ln0 > n {
			(*v)[0] = (*v)[0][n:]
			return
		}
		n -= ln0
		*v = (*v)[1:]
	}
}
