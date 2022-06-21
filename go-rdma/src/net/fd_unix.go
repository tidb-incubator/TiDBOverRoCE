// Copyright 2009 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

//go:build aix || darwin || dragonfly || freebsd || linux || netbsd || openbsd || solaris
// +build aix darwin dragonfly freebsd linux netbsd openbsd solaris

package net

import "C"
import (
	"context"
	"errors"
	"fmt"
	"internal/poll"
	"log"
	"os"
	"rdma"
	"runtime"
	"syscall"
)

const (
	readSyscallName     = "read"
	readFromSyscallName = "recvfrom"
	readMsgSyscallName  = "recvmsg"
	writeSyscallName    = "write"
	writeToSyscallName  = "sendto"
	writeMsgSyscallName = "sendmsg"
)

func newFD(sysfd, family, sotype int, net string) (*netFD, error) {
	ret := &netFD{
		pfd: poll.FD{
			Sysfd:         sysfd,
			IsStream:      sotype == syscall.SOCK_STREAM,
			ZeroReadIsEOF: sotype != syscall.SOCK_DGRAM && sotype != syscall.SOCK_RAW,
		},
		family: family,
		sotype: sotype,
		net:    net,
	}
	rdma.DPrintf("rdma debug: newFD %v ret isstream:%v zeroreadifeof:%v\n", ret.pfd.Sysfd, ret.pfd.IsStream, ret.pfd.ZeroReadIsEOF)
	return ret, nil
}

func (fd *netFD) init() error {
	return fd.pfd.Init(fd.net, true)
}

func (fd *netFD) name() string {
	var ls, rs string
	if fd.laddr != nil {
		ls = fd.laddr.String()
	}
	if fd.raddr != nil {
		rs = fd.raddr.String()
	}
	return fd.net + ":" + ls + "->" + rs
}

func waitWrite(f *netFD) (ret int, event int, err error) {
	pollerr := f.pfd.PollCheckerr('w')
	if pollerr != nil {
		return -1, 0, pollerr
		// break
	}
	// 在 waitWrite 之前执行的 connectFunc 可能返回EINPROGRESS，无法确定链接已经建立；
	// 需要再次对socket 执行writable 的监听继续判断
	ret, re, err := rdma.WrappedRpoll(f.pfd.Sysfd, rdma.POLLOUT, 0)
	return ret, re, err
}

func (fd *netFD) connect(ctx context.Context, la, ra syscall.Sockaddr) (rsa syscall.Sockaddr, ret error) {

	// syscall.connect 由于是对非阻塞fd 执行，会立即返回，此时若tcp 三次握手尚在进行，则errno 为EINPROGRESS
	if fd.pfd.IsDestroied {
		log.Printf("rdma connect: fd(%v) begin connect to %v, but use of closed network connection\n", fd.pfd.Sysfd, ra)
		return nil, errors.New("use of closed network connection")
	}
	// log.Printf("rdma connect: fd(%v) begin connect to %v\n",fd.pfd.Sysfd,ra)

	err := connectFunc(fd.pfd.Sysfd, ra)
	// log.Printf("rdma connect: fd.connect(%v) to %v connectFunc return err %v\n",fd.pfd.Sysfd,ra, err)

	switch err {
	case syscall.EINPROGRESS, syscall.EALREADY, syscall.EINTR:
		// EINPROGRESS 表示套接字为非阻塞套接字，且连接请求没有立即完成（可能尚处于tcp 三次握手环节）；
		// EALREADY 套接字为非阻塞套接字，并且原来的连接请求还未完成；
		// EINTR 表示有中断产生
		// log.Printf("rdma connect: fd.connect(%v) connectFunc return err %v\n",fd.pfd.Sysfd, err)
	case nil, syscall.EISCONN: // err 为nil 不会进入此case，此处nil有何作用？EISCONN  表示已经连接到该套接字
		select {
		case <-ctx.Done():
			log.Printf("rdma debug: connctFunc return nil/EISCONN, select case ctx.Done() fd %v err %v\n", fd.pfd.Sysfd, err)
			return nil, mapErr(ctx.Err())
		default:
			log.Printf("rdma debug: connctFunc return nil/EISCONN, select case default fd %v err %v\n", fd.pfd.Sysfd, err)
		}
		if err := fd.pfd.Init(fd.net, true); err != nil {
			log.Printf("rdma connect: fd.connect(%v) connectFunc return nil/EISCONN, pfd.Init return err:%v\n", fd.pfd.Sysfd, err)
			return nil, err
		}
		// 当前的 fd 被设为 nonblocking 模式
		// 若 fd 设置为 nonblocking，则忽略这里 Getpeername
		// 若 fd 为 blocking，则 connectFunc 会阻塞直到连接建立，
		// 因此会得到 syscall.EISCONN，此时需要 getpeername 并返回结果，表示 connect 成功执行完成
		// if rsa, err := rdma.Getpeername(fd.pfd.Sysfd); err == nil { //rdma modified
		// 	// log.Printf("rdma debug: fd.connect(%v) Getpeername return rsa %v, err %v\n",fd.pfd.Sysfd, rsa, err)
		// 	return rsa, nil
		// }
		// log.Printf("rdma connect: fd.connect(%v) connectFunc return nil/EISCONN err:%v\n",fd.pfd.Sysfd,err)
		runtime.KeepAlive(fd) // 避免 fd 被释放
		// log.Printf("rdma connect: fd.connect(%v) runtime.KeepAlive(%v)\n",fd.pfd.Sysfd,fd)
		return nil, nil
	case syscall.EINVAL:
		// log.Printf("rdma connect: fd.connect(%v) connectFunc return err:%v\n",fd.pfd.Sysfd,err)

		// On Solaris and illumos we can see EINVAL if the socket has
		// already been accepted and closed by the server.  Treat this
		// as a successful connection--writes to the socket will see
		// EOF.  For details and a test case in C see
		// https://golang.org/issue/6828.
		if runtime.GOOS == "solaris" || runtime.GOOS == "illumos" {
			return nil, nil
		}
		fallthrough
	default:
		// log.Printf("rdma connect: fd.connect(%v) connectFunc err default:%v\n",fd.pfd.Sysfd,err)

		return nil, os.NewSyscallError("connect", err)
	}
	// rdma modified: only nonblocking fd can reach here

	if err := fd.pfd.Init(fd.net, true); err != nil {
		return nil, err
	}

	if deadline, hasDeadline := ctx.Deadline(); hasDeadline {
		rdma.DPrintf("rdma debug: connct ctx.Deadline() Done fd %v deadline %v, hasDeadline %v\n", fd.pfd.Sysfd, deadline, hasDeadline)

		fd.pfd.SetWriteDeadline(deadline)
		defer fd.pfd.SetWriteDeadline(noDeadline)
	}
	// log.Printf("rdma debug: connct ctx.Deadline() Done fd %v d\n", fd.pfd.Sysfd)
	// Start the "interrupter" goroutine, if this context might be canceled.
	// (The background context cannot)
	//
	// The interrupter goroutine waits for the context to be done and
	// interrupts the dial (by altering the fd's write deadline, which
	// wakes up waitWrite).
	if ctx != context.Background() {
		// log.Printf("rdma connect: connct ctx != context.Background() fd %v ctx:%+v\n", fd.pfd.Sysfd,ctx)

		// Wait for the interrupter goroutine to exit before returning
		// from connect.
		done := make(chan struct{})
		interruptRes := make(chan error)
		defer func() {
			close(done)
			// log.Printf("rdma connect: closed done fd %v ctx:%v\n", fd.pfd.Sysfd,ctx)

			if ctxErr := <-interruptRes; ctxErr != nil && ret == nil {
				// The interrupter goroutine called SetWriteDeadline,
				// but the connect code below had returned from
				// waitWrite already and did a successful connect (ret
				// == nil). Because we've now poisoned the connection
				// by making it unwritable, don't return a successful
				// dial. This was issue 16523.
				ret = mapErr(ctxErr)
				fd.Close() // prevent a leak
			}
		}()
		go func() {
			select {
			case <-ctx.Done():
				// Force the runtime's poller to immediately give up
				// waiting for writability, unblocking waitWrite
				// below.
				fd.pfd.SetWriteDeadline(aLongTimeAgo)
				testHookCanceledDial()
				interruptRes <- ctx.Err()
			case <-done:
				interruptRes <- nil
			}
		}()
	} else {
		// log.Printf("rdma connect: fd.connect is background ctx\n")
	}

	// rdma fixme: rdma modified

	for !fd.pfd.IsDestroied { // fd.pfd.IsDestroied

		result, revent, err := waitWrite(fd) // rdma modified
		if result == 0 {
			// runtime.Gosched()
			continue
		}
		// result != 0
		// 两种可能：
		// 情况一： result == 1，表示 poll 到事件
		// 情况二： result !=1 且 result!=0，则 result 为 rpoll 中出错返回的-1，此时 err 记录了出错时的 errno

		if result == 1 { // 若rpoll 返回值为1，表示成功poll 到了fd 上的事件
			// 判断poll 到的事件中是否存在 pollerr 或pollup，若存在则 r!=0 ，后续代码可以根据r！=0 判断链接不再具备继续read/write 的条件
			// 若revents 中不存在pollerr 或pollup，则后续代码根据r==0 判断fd 上已产生了时间且可以继续read/write
			// pollup 表示tcp 连接已经收到FIN 挥手，即对端已断开链接；
			// pollerr 表示此fd 上后续io 会出错
			result = int(revent & (rdma.POLLERR | rdma.POLLHUP))
			// 若 waitWrite rpoll 监听到事件返回 1且事件不是 pollerr/pollhup, 则满足 result ==0
			if result == 0 {
				err = nil
			}
			// 否则 result 的值可能为 8 或 16
		}
		// log.Printf("rdma connect: do_poll(%v) result:%v err:%v\n",fd.pfd.Sysfd,result, err)
		// 此时 result 可能为 0 或 8 表示POLLERR (与pollerr|pollhup 进行按位与) 或 10 表示 POLLHUP；
		// 若为 0 则 err==nil；
		// 若不为 0，此时err 可能是connection refused，也可能是 transport endpoint is not connected，目前看并不固定
		if err != nil { // 链接已断开或出错，返回错误
			select {
			case <-ctx.Done():
				return nil, mapErr(ctx.Err())
			default:
			}
			// log.Printf("rdma connect: connect(%v) returning nil, err:%v\n",fd.pfd.Sysfd, err)
			return nil, err
			// return err
		}

		// errNo 用来存放socket 相关信息，此处用来获取 SO_ERROR信息，
		// rsocket 执行 rs_do_connect 和 rs_poll_cq 时 会将遇到的错误 errno 保存到 rs->err，
		// rgetsockopt 将rs->err 保存到 errNO 中
		var errNo int
		result, err = rdma.GetsockoptInt(fd.pfd.Sysfd, syscall.SOL_SOCKET, syscall.SO_ERROR, errNo) // rdma modified// 使用epoll 监听到writable 后，需要判断当前socket 的状态；若获取到errno 为零，才能说明socket connect成功
		// log.Printf("rdma connect: GetsockoptInt(%v) got errNo:%v, result:%v, err:%v\n",fd.pfd.Sysfd,errNo, result, err)
		// 此时可能 fd.pfd.IsDestoried 变为 true，导致 GetsockoptInt 返回 -1,bad file descripotr

		if result != 0 { // rsocket/rgetsockopt 执行出错返回 -1，同时错误信息存在 errno
			log.Printf("rdma connect: GetsockoptInt(%v) faild, need rclose according to riostream.c\n", fd.pfd.Sysfd)
			return nil, os.NewSyscallError("rdma getsockopt", err)
		}
		// result == 0，成功获取到 SO_ERROR，保存在 errNo 中
		// nerr = errNo
		switch err := syscall.Errno(errNo); err {
		case syscall.EINPROGRESS, syscall.EALREADY, syscall.EINTR: // 若为此三种状态，需要继续循环判断writable
			log.Printf("rdma debug:GetsockoptInt(%v) got errNo:%v, retruning %v\n", fd.pfd.Sysfd, err, ret)
			runtime.Gosched()
		case syscall.EISCONN:
			log.Printf("rdma debug:GetsockoptInt(%v) got errNo:%v, retruning nil nil\n", fd.pfd.Sysfd, err)
			return nil, nil
		case syscall.Errno(0): // 判断getsocketopt 返回的errno 为0，表示connect 成功
			// log.Printf("rdma connect: GetsockoptInt(%v) got errNo:0\n",fd.pfd.Sysfd)

			// The runtime poller can wake us up spuriously;
			// see issues 14548 and 19289. Check that we are
			// really connected; if not, wait again.
			lsa, _ := rdma.Getsockname(fd.pfd.Sysfd)
			if rsa, err := rdma.Getpeername(fd.pfd.Sysfd); err == nil { //rdma modified
				// log.Printf("rdma connect: rdma.Getpeername(%v) rsa:%v lsa:%v\n",fd.pfd.Sysfd, rsa, lsa)

				// 当向虚拟网卡的 192.168.122.1 发起connect 请求时，可能无法正常得到 rejected 响应
				// 但此时 local addr 为 0，port 也为 0，而正常情况下此时 local addr 和 port 为非 0 值
				// 因此使用 lsa.Port == 0 判断此异常并返回给 connect 的调用者
				switch sa := lsa.(type) {
				case *syscall.SockaddrInet4:
					if sa.Port == 0 {
						log.Printf("rdma connect: rdma.Getsockname(%v) returning nil, ENOTCONN rsa:%v lsa:%v\n", fd.pfd.Sysfd, rsa, lsa)
						return nil, syscall.ENOTCONN
					}
				}
				// log.Printf("rdma connect: rdma.Getpeername(%v) returning rsa:%v, nil\n",fd.pfd.Sysfd, rsa)

				return rsa, nil
			}
		default:
			log.Printf("rdma connect: connect(%v) returning nil, err:%v\n", fd.pfd.Sysfd, err)
			return nil, os.NewSyscallError("rdma connect", err)
		}
		runtime.KeepAlive(fd)
		// log.Printf("rdma connect: runtime.KeepAlive(%v)\n", fd)
	}
	// fd.pfd.IsDestroied == true
	return nil, os.NewSyscallError("rdma connect", errors.New("use of closed network connection"))
}
func fAccept(lfd *netFD) (int, syscall.Sockaddr, string, error) {

	ns, sa, err := rdma.Accept(lfd.pfd.Sysfd)

	// fmt.Printf("rdma debug: rdma fAccept rdma.Accept %v return ns:%d sa:%v err:%v\n", lfd.pfd.Sysfd, ns, sa, err)

	if ns < 0 {
		// err may be BADF or EAGAIN
		if err != syscall.EAGAIN {
			log.Printf("rdma ERROR: rdma fAccept rdma.Accept %v return ns:%d sa:%v err:%v\n", lfd.pfd.Sysfd, ns, sa, err)
		}
		return ns, nil, "accept", err
	}
	if ns == 0 {
		panic(fmt.Sprintf("netFD Accept ns:%d err:%v", ns, err))
	}
	// rdma.CloseOnExec(ns)

	if ret := rdma.SetNonblock(ns, true); ret != 0 {
		log.Printf("rdma debug: rdma fAccept(%v) got ns:%d but SetNonblock ret:%v\n", lfd.pfd.Sysfd, ns, ret)

		rdma.CloseFunc(ns)
		err = errors.New("errno" + itoa(ret))
		return -1, nil, "setnonblock", os.NewSyscallError("setnonblock", err)
	}
	// fmt.Printf("rdma debug: rdma fAccept %v success return ns:%d sa:%v err:%v\n", lfd.pfd.Sysfd, ns, sa, err)
	return ns, sa, "", nil
}

func pfdAccept(listenfd *netFD) (newfd int, newSa syscall.Sockaddr, errString string, err error) {

	if err := listenfd.ReadLock(); err != nil {
		// log.Printf("rdma debug: rdma fAccept ReadLock() fd %v ,err %v \n", listenfd.pfd.Sysfd, err)
		return -1, nil, "", err
	}
	// log.Printf("rdma debug: pfdAccept(%v) acquired ReadLock\n", listenfd.pfd.Sysfd)

	//此处减少fd.fdmu的引用计数。accept建立后的连接并不受监听socket控制，即使close监听socket，已有的连接会不会被关闭。
	//此处的readUnlock还有一个作用，在accept失败后，会在引用计数为0且fd.fdmu状态为mutexClosed时会调用poll.destory()函数close该连接。
	//从代码层面看，只能通过poll.Close(net/fd_unix.go)将网络描述符的fd.fdmu设置为mutexClose，该函数为对外API.
	//如果fd.fdmu未关闭，则此处仅仅减少引用计数。
	defer func() {
		listenfd.ReadUnlock()
		// log.Printf("rdma debug: fAccept(%v) release ReadLock\n", listenfd.pfd.Sysfd)
	}()

	if err := listenfd.pfd.PrepareRead(false); err != nil {
		log.Printf("rdma debug: rdma fAccept PrepareRead(%v) ,err %v \n", listenfd.pfd.Sysfd, err)
		return -1, nil, "", err
	}
	// log.Printf("rdma debug: pfdAccept(%v) enter loop\n", listenfd.pfd.Sysfd)

	for !listenfd.pfd.IsDestroied {
		s, rsa, errcall, err := fAccept(listenfd)
		if s > 0 {
			// log.Printf("rdma accept: rdma pfdAccept(%d) return %v %v \n", listenfd.pfd.Sysfd, s,rsa)
			return s, rsa, "", nil
		}
		if s == 0 {
			panic("netFD pfdAccept")
		}
		// s < 0
		if err == syscall.EINTR || err == syscall.EAGAIN || err == syscall.ECONNABORTED {
			// log.Printf("rdma debug: rdma accept %d return %v ,err %v \n", listenfd.pfd.Sysfd, s, err)
			switch err {
			case syscall.EINTR:
				// log.Printf("rdma debug: rdma accept %d return EINTR %v ,pollable:%v will try accept again \n", listenfd.pfd.Sysfd, err, pollable)
				continue
			case syscall.EAGAIN:
				// log.Printf("rdma debug: rdma accept %d return EAGAIN %v ,pollable:%v will rpoll \n", listenfd.pfd.Sysfd, err, pollable)
				if listenfd.pfd.Pollable() {
					ret := 0
					re := 0
					err = listenfd.pfd.PollCheckerr('r')
					if err != nil {
						// log.Printf("rdma debug: rdma accept %d PollCheckerr %v ,ret -1 \n", listenfd.pfd.Sysfd, err)
						return -1, nil, errcall, err
						// break
					}
					ret, re, err = rdma.WrappedRpoll(listenfd.pfd.Sysfd, syscall.EPOLLIN, 0)
					if ret == 0 {
						// runtime.Gosched()
						continue
					}
					// log.Printf("rdma debug: rdma accept %d polled return %d r.Revents %v err:%v \n", listenfd.pfd.Sysfd, ret, re, err)

					if ret == 1 { // 若rpoll 返回值为1，表示成功poll 到了fd 上的事件
						// 判断poll 到的事件中是否存在 pollerr 或pollup，若存在则 r!=0 ，后续代码可以根据r！=0 判断链接不再具备继续read/write 的条件
						// 若revents 中不存在pollerr 或pollup，则后续代码根据r==0 判断fd 上已产生了时间且可以继续read/write
						ret = re & (rdma.POLLERR | rdma.POLLHUP) // pollup 表示tcp 连接已经收到FIN 挥手，即对端已断开链接；pollerr 表示此fd 上后续io 会出错

					}
					if ret != 0 { // 链接已断开或出错，返回错误
						log.Printf("rdma accept: accept(%d) do_poll ret:%d ,pollable:%v will return \n", listenfd.pfd.Sysfd, ret, listenfd.pfd.Pollable())
						return ret, nil, "accept polled pollerr or pollup", err // rdma fixme: ret -1?
						// return -1, nil, errcall, err
					}
					// log.Printf("rdma debug: rdma accept %d polled %d ,pollable:%v will continue rpoll \n", listenfd.pfd.Sysfd, ret, pollable)
					continue
				} else {
					log.Printf("rdma accept: accept(%d) returning -1 cause pd not pollable, err:%v.errcall:%v\n", listenfd.pfd.Sysfd, err, errcall)

					return -1, nil, "accept", errors.New("pd not pollable")
				}
			case syscall.ECONNABORTED:
				// log.Printf("rdma debug: rdma accept %d return ECONNABORTED %v ,pollable:%v will try accept again \n", listenfd.pfd.Sysfd, err, pollable)
				// runtime.Gosched()
				continue
			default:
				// runtime.Gosched()
			}
			log.Printf("rdma accept: accept(%d) returning -1 because switch into default, err:%v.errcall:%v\n", listenfd.pfd.Sysfd, err, errcall)
			return -1, nil, errcall, err
		} else {
			log.Printf("rdma accept: accept(%v) returning -1 because ret:%v and unknow err %v \n", listenfd.pfd.Sysfd, s, err)
			return -1, nil, errcall, err
		}
	}
	log.Printf("rdma debug: pfdAccept(%v) skip loop\n", listenfd.pfd.Sysfd)
	// pfd.IsDestroied == true
	return -1, nil, "use of closed network connection", errors.New("use of closed network connection")
}

func (fd *netFD) accept() (netfd *netFD, err error) {
	// log.Printf("rdma accept: netFD accept(%d) start\n", fd.pfd.Sysfd)

	// 原accept 逻辑代码
	// d, rsa, errcall, err := fd.pfd.Accept()
	// if err != nil {
	// 	if errcall != "" {
	// 		err = wrapSyscallError(errcall, err)
	// 	}
	// 	return nil, err
	// }

	// if netfd, err = newFD(d, fd.family, fd.sotype, fd.net); err != nil {
	// 	poll.CloseFunc(d)
	// 	return nil, err
	// }
	// if err = netfd.init(); err != nil {
	// 	netfd.Close()
	// 	return nil, err
	// }
	// lsa, _ := syscall.Getsockname(netfd.pfd.Sysfd)
	// netfd.setAddr(netfd.addrFunc()(lsa), netfd.addrFunc()(rsa))
	// return netfd, nil

	// rdma modified start, using rpoll

	d, rsa, errcall, err := pfdAccept(fd)
	// log.Printf("rdma accept: netFD accept pfdAccept(%d) return  %v %v %v %v\n", fd.pfd.Sysfd, d, rsa, errcall, err)

	if d < 0 {
		if errcall != "" {
			err = wrapSyscallError(errcall, err)
		}
		// log.Printf("rdma debug: rdma accept pfdAccept %d ERROR, %v %v %v %v\n", fd.pfd.Sysfd, d, rsa, errcall, err)
		return nil, err
	}

	if netfd, err = newFD(d, fd.family, fd.sotype, fd.net); err != nil {
		rdma.CloseFunc(d)
		return nil, err
	}
	if err = netfd.init(); err != nil {
		// log.Printf("rdma debug: Accept netfd init err fd:%v\n", fd.pfd.Sysfd)
		netfd.Close()
		return nil, err
	}
	lsa, _ := rdma.Getsockname(netfd.pfd.Sysfd)
	netfd.setAddr(netfd.addrFunc()(lsa), netfd.addrFunc()(rsa))
	// log.Printf("rdma accept: Accept Done listen fd:%v ,new fd %v, rsa: %v, lsa: %v\n", fd.pfd.Sysfd, netfd.pfd.Sysfd, rsa, lsa)

	return netfd, nil
}

func (fd *netFD) dup() (f *os.File, err error) {
	ns, call, err := fd.pfd.Dup()
	if err != nil {
		if call != "" {
			err = os.NewSyscallError(call, err)
		}
		return nil, err
	}

	return os.NewFile(uintptr(ns), fd.name()), nil
}
