// Copyright 2015 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

//go:build aix || darwin || dragonfly || freebsd || (js && wasm) || linux || netbsd || openbsd || solaris
// +build aix darwin dragonfly freebsd js,wasm linux netbsd openbsd solaris

package net

import (
	"rdma"
	"syscall"
)

var (
	testHookDialChannel  = func() {} // for golang.org/issue/5349
	testHookCanceledDial = func() {} // for golang.org/issue/16523

	// Placeholders for socket system calls.
	socketFunc        func(int, int, int) (int, error)  = rdma.Socket  // syscall.Socket  // rdma modified//
	connectFunc       func(int, syscall.Sockaddr) error = rdma.Connect //syscall.Connect // rdma modified//
	listenFunc        func(int, int) error              = rdma.Listen  //syscall.Listen  // rdma modified//
	getsockoptIntFunc func(int, int, int) (int, error)  = syscall.GetsockoptInt
)
