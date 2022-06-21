// Copyright 2020 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

//go:build aix || darwin || dragonfly || freebsd || linux || netbsd || openbsd || solaris || windows
// +build aix darwin dragonfly freebsd linux netbsd openbsd solaris windows

package net

import (
	"errors"
	"internal/poll"
	"rdma"
	"runtime"
	"syscall"
	"time"
	// "fmt"
	// "log"
)

// Network file descriptor.
type netFD struct {
	pfd poll.FD

	// immutable until Close
	family      int
	sotype      int
	isConnected bool // handshake completed or use of association with peer
	net         string
	laddr       Addr
	raddr       Addr
}

func (fd *netFD) setAddr(laddr, raddr Addr) {
	fd.laddr = laddr
	fd.raddr = raddr
	runtime.SetFinalizer(fd, (*netFD).Close)
	rdma.DPrintf("rdma debug: netFD setAddr() fd %v laddr %v raddr %v\n", fd, laddr, raddr)
}

var ErrNetClosing = errors.New("use of closed network connection")

// ErrFileClosing is returned when a file descriptor is used after it
// has been closed.
var ErrFileClosing = errors.New("use of closed file")

// Return the appropriate closing error based on isFile.
func errClosing(isFile bool) error {
	if isFile {
		return ErrFileClosing
	}
	return ErrNetClosing
}

// rdma added
func Destroy(pfd *poll.FD) error {

	pfd.PdClose() // 执行 epollctl( ,_EPOLL_CTL_DEL,) 将 fd 从 epoll 监听队列删除，并将 pd.runtimeCtx 置为 0
	// log.Printf("rdma close: Destroy(%d) PdClose done\n", pfd.Sysfd)

	// err := CloseFunc(fd.Sysfd)// 原本是调用 syscall.Close
	err := rdma.RClose(pfd.Sysfd) // 调整为调用 rdma rclose
	// log.Printf("rdma close: RClose(%d) rdma.RClose return:%v\n", pfd.Sysfd,err)

	pfd.Sysfd = -1
	pfd.Runtime_Semrelease()
	return err
}

// rdma added
func Decref(pfd *poll.FD) error {
	if pfd.FdmuDecref() {
		return Destroy(pfd)
	}
	return nil
}

// rdma added
func RClose(pfd *poll.FD) error {
	// log.Printf("rdma close: RClose(%d) start\n", pfd.Sysfd)

	if !pfd.FdmuIncrefAndClose() {
		return pfd.ErrClosing()
	}
	pfd.PdEvict()
	pfd.IsDestroied = true

	err := Decref(pfd)

	if pfd.IsBlocking() == 0 {
		pfd.Runtime_Semacquire()
	}
	return err
}

// rdma added
// export readLock
func (fd *netFD) ReadLock() error {
	if !fd.pfd.FdmuRwlock(true) {
		return errClosing(fd.pfd.IsFile())
	}
	return nil
}

// rdma added
// copy from internal/poll/fd_mutex.go readUnlock
func (fd *netFD) ReadUnlock() {
	if fd.pfd.FdmuRwunlock(true) {
		// log.Printf("rdma close: ReadUnlock will Destroy(%d)\n", fd.pfd.Sysfd)

		Destroy(&(fd.pfd))
		// fd.destroy()
	}
}

// rdma added
// export writeLock
func (fd *netFD) WriteLock() error {
	if !fd.pfd.FdmuRwlock(false) {
		// print("WriteLock fd ",fd.Sysfd," fd.fdmu.rwlock(false) return false\n")
		return errClosing(fd.pfd.IsFile())

	}
	// print("WriteLock fd ",fd.Sysfd," fd.fdmu.rwlock(false) return true\n")

	return nil
}

// rdma modified
// export writeUnlock
func (fd *netFD) WriteUnlock() {
	if fd.pfd.FdmuRwunlock(false) {
		// log.Printf("rdma close: WriteUnlock will Destroy(%d)\n", fd.pfd.Sysfd)
		Destroy(&(fd.pfd))

		// fd.destroy()
	}
}

// rdma modified
func (fd *netFD) Close() error {
	// log.Printf("rdma close: netFD(%d) Close start\n", fd.pfd.Sysfd)
	runtime.SetFinalizer(fd, nil)
	// log.Printf("rdma debug: runtime.SetFinalizer(%d, nil) done\n", fd.pfd.Sysfd)
	// toRClose := fd.pfd.Sysfd
	err := RClose(&(fd.pfd))
	// log.Printf("rdma debug: netFD Close %v to %v, err:%v\n", toRClose,fd.pfd.Sysfd, err)
	return err

}

func (fd *netFD) shutdown(how int) error {
	// log.Printf("rdma debug: netFD shutdown(%d) start\n", fd.pfd.Sysfd)
	err := fd.pfd.Shutdown(how)
	runtime.KeepAlive(fd)
	return wrapSyscallError("shutdown", err)
}

func (fd *netFD) closeRead() error {
	return fd.shutdown(syscall.SHUT_RD)
}

func (fd *netFD) closeWrite() error {
	return fd.shutdown(syscall.SHUT_WR)
}

func (fd *netFD) Read(p []byte) (n int, err error) {
	rdma.DPrintf("rdma debug: netFD Read() %v\n", fd)
	n, err = fd.pfd.Read(p)
	runtime.KeepAlive(fd)
	return n, wrapSyscallError(readSyscallName, err)
}

func (fd *netFD) readFrom(p []byte) (n int, sa syscall.Sockaddr, err error) {
	n, sa, err = fd.pfd.ReadFrom(p)
	runtime.KeepAlive(fd)
	return n, sa, wrapSyscallError(readFromSyscallName, err)
}

func (fd *netFD) readMsg(p []byte, oob []byte) (n, oobn, flags int, sa syscall.Sockaddr, err error) {
	n, oobn, flags, sa, err = fd.pfd.ReadMsg(p, oob)
	runtime.KeepAlive(fd)
	return n, oobn, flags, sa, wrapSyscallError(readMsgSyscallName, err)
}

func (fd *netFD) Write(p []byte) (nn int, err error) {
	rdma.DPrintf("rdma debug: netFD Write() %v\n", fd)
	nn, err = fd.pfd.Write(p)
	runtime.KeepAlive(fd)
	return nn, wrapSyscallError(writeSyscallName, err)
}

func (fd *netFD) writeTo(p []byte, sa syscall.Sockaddr) (n int, err error) {
	n, err = fd.pfd.WriteTo(p, sa)
	runtime.KeepAlive(fd)
	return n, wrapSyscallError(writeToSyscallName, err)
}

func (fd *netFD) writeMsg(p []byte, oob []byte, sa syscall.Sockaddr) (n int, oobn int, err error) {
	n, oobn, err = fd.pfd.WriteMsg(p, oob, sa)
	runtime.KeepAlive(fd)
	return n, oobn, wrapSyscallError(writeMsgSyscallName, err)
}

func (fd *netFD) SetDeadline(t time.Time) error {
	return fd.pfd.SetDeadline(t)
}

func (fd *netFD) SetReadDeadline(t time.Time) error {
	return fd.pfd.SetReadDeadline(t)
}

func (fd *netFD) SetWriteDeadline(t time.Time) error {
	return fd.pfd.SetWriteDeadline(t)
}
