package rdma

/*
#cgo CFLAGS: -g
#cgo LDFLAGS: -lrdmacm -libverbs -L/home/tidb/software_rdma/rdma-core/build/lib  -Wl,-rpath,/home/tidb/software_rdma/rdma-core/build/lib
#include <stdint.h>
#include <stdio.h>
#include <fcntl.h>
#include <netinet/tcp.h>
#include <sys/epoll.h>
#include <poll.h>
#include <errno.h>

#include <rdma/rdma_cma.h>
#include <rdma/rsocket.h>
#include <sys/socket.h>
#include <stdlib.h>

enum rs_optimization {
	opt_mixed,
	opt_latency,
	opt_bandwidth
};

void CloseOnExec(int fd,int cmd,int flag){
	fcntl(fd,cmd,flag);
}

int SetNonblock(int fd,int nonblocking){

	int flag = rfcntl(fd, F_GETFL, 0);
	if (flag < 0) {
		return flag;
	}
	// if (flag & O_NONBLOCK) {
    //     return 0;
    // }
	if (nonblocking){
		flag |= O_NONBLOCK;
	}else{
		flag = flag &~ O_NONBLOCK;
	}

	int ret = rfcntl(fd,F_SETFL,flag);
	return ret;
}

int Rfcntl(int fd, int cmd, int flag){
	return rfcntl(fd,cmd,flag);
}

int CloseFunc(int fd){
	return rclose(fd);
}

int SetsockoptInt(int fd,int level, int optname ,const void *optval, socklen_t optlen){
 	int val = 1;
	const void * pval = &val;
 	return rsetsockopt(fd, level, optname, pval, sizeof(val));
}

int Close(int socket){
	return rclose(socket);
}

static void set_options(int fd)
{
	int val;

	val = 1;

	rsetsockopt(fd, IPPROTO_TCP, TCP_NODELAY, (void *) &val, sizeof(val));
	rsetsockopt(fd, SOL_RDMA, RDMA_IOMAPSIZE, (void *) &val, sizeof val);

	//SetNonblock(fd);

}

*/
import "C"
import (
	"syscall"
	"time"
	"unsafe"
)

// 提供 rdma modified 所需的主要api

// rdma 相关debug 日志开关
const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		// log.Printf(format, a...)
	}
	return
}

var SocketDisableIPv6 bool

const (
	AF_INET6 = 0xa

	SOCK_STREAM  = 0x1
	ENODEV       = syscall.Errno(0x13)
	EAFNOSUPPORT = syscall.Errno(0x2f)

	_EPOLLIN       = 0x1
	_EPOLLOUT      = 0x4
	_EPOLLERR      = 0x8
	_EPOLLHUP      = 0x10
	_EPOLLRDHUP    = 0x2000
	_EPOLLET       = 0x80000000
	_EPOLL_CLOEXEC = 0x80000
	_EPOLL_CTL_ADD = 0x1
	_EPOLL_CTL_DEL = 0x2
	_EPOLL_CTL_MOD = 0x3
	EPOLLET        = C.EPOLLET

	POLLIN  = 0x1
	POLLOUT = 0x4
	POLLERR = 0x8
	POLLHUP = 0x10

	F_GETFL = 0x3
)

var (
	errEAGAIN error = syscall.EAGAIN
	errEINVAL error = syscall.EINVAL
	errENOENT error = syscall.ENOENT
)

func errnoErr(e syscall.Errno) error {
	switch e {
	case 0:
		return nil
	case syscall.EAGAIN:
		return errEAGAIN
	case syscall.EINVAL:
		return errEINVAL
	case syscall.ENOENT:
		return errENOENT
	}
	return e
}

func Socket(domain, typ, proto int) (fd int, err error) {
	cfd, err := C.rsocket(C.int(domain), C.int(typ), C.int(proto))
	fd = int(cfd)
	// log.Printf("rdma debug: rdma socket return fd:%d, err:%v", fd, err)

	if fd >= 0 {
		// seems like C.rsocket()
		// returns unexpected errno 2(ENOENT) which means "no such file or directory"
		// while return fd success,
		// and ingnore this is ok for now
		err = nil
	} else if err == syscall.EOPNOTSUPP {
		// seems like rsocket() didn't support SOCK_NONBLOCK and SOCK_CLOEXEC flags when creating socket
		// and will return  errno 95(EOPNOTSUPP, redefined to ENOTSUP in roscket.c)
		// but upper caller net/sysSocket() didn't handle EOPNOTSUPP
		// so change err to adjust net/sysSocket()
		err = syscall.EINVAL
	}
	return
}

func CloseOnExec(fd int) {
	C.CloseOnExec(C.int(fd), C.int(syscall.F_SETFD), C.int(syscall.FD_CLOEXEC))
}

func SetNonblock(fd int, nonblocking bool) (errNo int) {
	// log.Printf("rdma SetNonblock start ,fd:%v\n", fd)
	wantnonblock := 0
	if nonblocking {
		wantnonblock = 1
	}

	ret, err := C.SetNonblock(C.int(fd), C.int(wantnonblock))
	// log.Printf("rsocket SetNonblock fd:%v  ret:%v err:%v\n", fd, ret, ret)
	if int(ret) != 0 {
		errNo = -1 * int(err.(syscall.Errno))
	} else {
		errNo = 0
	}
	return errNo
}

func GetsockoptInt(fd, level, opt int, value int) (ret int, err error) {
	var l C.socklen_t = syscall.SizeofSockaddrAny
	// rgetsockopt 返回值：
	// 当正常执行时，返回值为 0，要获取的socket 信息被保存到value 中，不需要关注 errno；
	// 当执行出错时，返回值被置为 -1，错误信息被保存到 errno 中；
	c_ret, err := C.rgetsockopt(C.int(fd), C.int(level), C.int(opt), unsafe.Pointer(&value), &l)
	// 若 c_ret == 0，则value 中保存了要获取的 socket 的信息
	// 若 c_ret == -1，则err 为错误信息
	ret = int(c_ret)
	if ret == 0 { // 若 rgetsockopt 返回0，说明正常执行，忽略 errno
		return ret, nil
	}
	return ret, err // 否则返回 -1，errno
}

func SetsockoptInt(fd, level, opt int, value int) (err error) {
	ret, err := C.SetsockoptInt(C.int(fd), C.int(level), C.int(opt), unsafe.Pointer(&value), 4)
	// log.Printf("rdma debug: rdma SetsockoptInt return ,fd:%v\n ret:%v err:%v", fd, ret, err)

	if ret == 0 {
		return nil
	}
	return err
}

func SetKeepAlive(fd int, keepalive bool) (err error) {
	optval := 1
	ret, err := C.SetsockoptInt(C.int(fd), C.int(syscall.SOL_SOCKET), C.int(syscall.SO_KEEPALIVE), unsafe.Pointer(&optval), 4)
	// log.Printf("rdma debug: rdma SOL_SOCKET SO_KEEPALIVE return ,fd:%v\n ret:%v err:%v", fd, ret, err)

	if ret != 0 {
		return err
	}
	// runtime.KeepAlive(fd)
	return nil
}
func roundDurationUp(d time.Duration, to time.Duration) time.Duration {
	return (d + to - 1) / to
}

func SetKeepAlivePeriod(fd int, d time.Duration) (err error) {

	secs := int(roundDurationUp(d, time.Second))
	keepaliveTime := secs
	var ret C.int
	if ret, err = C.SetsockoptInt(C.int(fd), C.int(syscall.IPPROTO_TCP), C.int(syscall.TCP_KEEPINTVL), unsafe.Pointer(&keepaliveTime), 4); ret != 0 {
		return err
	}

	if ret, err = C.SetsockoptInt(C.int(fd), C.int(syscall.IPPROTO_TCP), C.int(syscall.TCP_KEEPIDLE), unsafe.Pointer(&keepaliveTime), 4); ret != 0 {
		return err
	}
	// log.Printf("rdma debug: rdma IPPROTO_TCP TCP_KEEPIDLE return ,fd:%v\n ret:%v err:%v", fd, ret, err)

	// runtime.KeepAlive(fd)
	return
}

func Rset_options(fd int) {
	C.set_options(C.int(fd))
}

func CloseFunc(fd int) (err error) {
	ret, err := C.CloseFunc(C.int(fd))
	if C.int(ret) != 0 {
		return err
	}
	return nil
}

func Bind(fd int, sa syscall.Sockaddr) (err error) {
	switch sa := sa.(type) {
	case *syscall.SockaddrInet4:
		rsa := syscall.RawSockaddrInet4{Family: syscall.AF_INET, Port: btol(uint16(sa.Port))} // TODO: need to fix Big/Little Endian problem（want bind 11000011 10000011，but actually bind 10000011 11000011 ）
		ip := sa.Addr[:]
		copy(rsa.Addr[:], ip)
		c_sa := (*C.struct_sockaddr)(unsafe.Pointer(&rsa))
		ret, err := C.rbind(C.int(fd), c_sa, C.socklen_t(syscall.SizeofSockaddrInet4))
		// log.Printf("rdma debug: rdma Bind fd: %v ret:%v, err:%v\n", fd, ret, err)
		if ret != 0 {
			return err
		}
		return nil
	default:
		return EAFNOSUPPORT
	}
}

func btol(be uint16) (se uint16) {
	p := (*[2]byte)(unsafe.Pointer(&be))
	se = uint16(p[0])<<8 + uint16(p[1])
	// se = ((be & 0xff) << 8) + (be >> 8)
	return
}

func Listen(fd, backlog int) (err error) {
	// cfd := fd
	ret, err := C.rlisten(C.int(fd), C.int(backlog))
	// log.Printf("rdma debug: rdma Listen fd: %v ret:%v, err:%v\n", cfd, ret, err)
	if ret != 0 {
		return err
	}
	return nil
}

// return -1,nil,errno when raccept failed
// return new_rs->cm_id->channel->fd,sa,nil when raccept and andToSockaddr succeed
func Accept(socket int) (nfd int, sa syscall.Sockaddr, err error) {
	// log.Printf("rdma debug: rdma accept start\n")
	// flag, err := C.Rfcntl(C.int(socket), C.int(F_GETFL), 0)
	// log.Printf("rdma debug: rdma Accept fd: %v F_GETFL flag:%v, err:%v\n", socket, flag, err)
	var rsa syscall.RawSockaddrAny
	// c_sa := (*C.struct_sockaddr)(unsafe.Pointer(&rsa))
	var len C.socklen_t = syscall.SizeofSockaddrAny
	ret, err := C.raccept(C.int(socket), (*C.struct_sockaddr)(unsafe.Pointer(&rsa)), &len) // rsocket raccept() may return rs->index which >0 when succeed, and may return -1 with errno if failed

	if ret < 0 {
		// 可能是 EAGAIN
		// log.Printf("rdma INFO: rsocket raccept(%d) ret:%d err:%v\n", socket, ret, err)
		return int(ret), nil, err
	}
	// ret >=0 时还有两种可能情况：
	// 情况一：		errno == ERR(EFAULT)，读取到异常的 new_rs
	// 情况二：		errno != ERR(EFAULT)，经常为 ERR(0)，读取到正常的 new_rs

	if err == syscall.EFAULT {
		panic(err)
	}
	if ret == 0 {
		panic(err)
	}
	// ret >0 且 errno != EFAULT

	// todo(zhangpc) 这里对 rsa 是否可能存在 race，导致 rsa 在进入 anyToSockaddr 之前值被修改？
	if rsa.Addr.Family != syscall.AF_INET {
		return int(ret), nil, err
	}
	err = nil
	sa, err = anyToSockaddr(&rsa)
	if err != nil {
		// log.Printf("rdma ERROR: rdma accept(%d) anyToSockaddr ret:%d err:%v\n", socket, ret, err)

		// C.Close(C.int(ret))
		// nfd = 0
		return 0, nil, err
	}
	// nfd = int(ret)
	// log.Printf("rdma debug: rdma accept done nfd:%v sa:%v err:%v\n", nfd, sa, err)
	return int(ret), sa, nil
}

func Accept4(socket, flag int) (int, syscall.Sockaddr, error) {
	// return ENOSYS for using  accept() syscall rather than accept4()
	return 0, nil, syscall.ENOSYS
}

func Connect(socket int, sa syscall.Sockaddr) (err error) {
	switch sa := sa.(type) {
	case *syscall.SockaddrInet4:

		rsa := syscall.RawSockaddrInet4{Family: syscall.AF_INET, Port: btol(uint16(sa.Port))}
		ip := sa.Addr[:]
		copy(rsa.Addr[:], ip)
		c_sa := (*C.struct_sockaddr)(unsafe.Pointer(&rsa))

		// SetNonblock(socket, false) //使用阻塞方式connect
		// flag, err = C.Rfcntl(C.int(socket), C.int(F_GETFL), 0)
		// log.Printf("rdma debug: rdma Connect before rconnect fd: %v F_GETFL flag:%v, err:%v\n", socket, flag, err)
		ret, err := C.rconnect(C.int(socket), c_sa, C.socklen_t(syscall.SizeofSockaddrAny))
		// SetNonblock(socket, true) //重新设置fd 为非阻塞
		// flag, err = C.Rfcntl(C.int(socket), C.int(F_GETFL), 0)
		// log.Printf("rdma debug: rdma Connect(%d) after rconnect ret %v err:%v\n", socket, ret, err)
		if ret != 0 {
			// log.Printf("rdma debug: rdma connect ret:%v err:%v\n", ret, err)
			return err
		}
		// log.Printf("rdma debug: rdma connect return nil \n")
		return nil

	default:
		// log.Printf("rdma debug: rdma connect return EAFNOSUPPORT \n")
		return EAFNOSUPPORT
	}
}

func anyToSockaddr(rsa *syscall.RawSockaddrAny) (syscall.Sockaddr, error) {
	switch rsa.Addr.Family {

	case syscall.AF_INET:
		pp := (*syscall.RawSockaddrInet4)(unsafe.Pointer(rsa))
		sa := new(syscall.SockaddrInet4)
		p := (*[2]byte)(unsafe.Pointer(&pp.Port))
		sa.Port = int(p[0])<<8 + int(p[1])
		for i := 0; i < len(sa.Addr); i++ {
			sa.Addr[i] = pp.Addr[i]
		}
		return sa, nil

	}
	panic(rsa.Addr.Family)
	// log.Printf("rdma debug: rdma anyToSockaddr got rsa:%v\n", rsa)
	// return nil, EAFNOSUPPORT
}

func RWrite(fd int, b []byte) (n int, err error) {
	ret, err := C.rwrite(C.int(fd), unsafe.Pointer(&b[0]), C.size_t(len(b)))
	return int(ret), err
}

func RRead(fd int, b []byte) (n int, err error) {
	ret, err := C.rread(C.int(fd), unsafe.Pointer(&b[0]), C.size_t(len(b)))
	return int(ret), err
}

type Pollfd struct {
	Fd      int
	Events  int16
	Revents int16
}

func Rpoll(fds []Pollfd, nfds int, timeout int) (int, error) {
	// log.Printf("fds %p ,fds[0]:%p ,len(fds):%v", &fds, &fds[0], len(fds))
	n, err := C.rpoll((*C.struct_pollfd)(unsafe.Pointer(&fds)), C.ulong(nfds), C.int(timeout))
	return int(n), err
}

func WrappedRpoll(fd int, e int, timeout int) (n int, re int, err error) {
	rfd := new(C.struct_pollfd)
	rfd.fd = C.int(fd)
	rfd.events = C.short(e)
	rfd.revents = 0
	ret, err := C.rpoll(rfd, 1, C.int(int32(timeout)))
	return int(ret), int(rfd.revents), err
}

func Getsockname(fd int) (sa syscall.Sockaddr, err error) {
	var rsa syscall.RawSockaddrAny
	c_sa := (*C.struct_sockaddr)(unsafe.Pointer(&rsa))
	var len C.socklen_t = syscall.SizeofSockaddrAny
	_, err = C.rgetsockname(C.int(fd), c_sa, &len)
	if err != nil {
		return nil, err
	}
	sa, err = anyToSockaddr(&rsa)
	if err != nil {
		return sa, err
	}
	return
}

func Getpeername(fd int) (sa syscall.Sockaddr, err error) {
	var rsa syscall.RawSockaddrAny
	c_sa := (*C.struct_sockaddr)(unsafe.Pointer(&rsa))
	var len C.socklen_t = syscall.SizeofSockaddrAny
	_, err = C.rgetpeername(C.int(fd), c_sa, &len)
	if err != nil {
		return nil, err
	}
	sa, err = anyToSockaddr(&rsa)
	if err != nil {
		panic(err)
		// return sa, err
	}
	return
}

func RClose(fd int) error {
	ret, err := C.rclose(C.int(fd))
	// log.Printf("rdma debug: rdma RClose end fd: %v ret:%v, err:%v\n", fd, ret, err)
	if ret == 0 {
		return nil
	} else {
		return err
	}
}
