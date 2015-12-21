package fdfs_client

import (
	"errors"
	"fmt"
	"io"
	"math/rand"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

const (
	DialRetryTimes    = 10
	ReceiveRetryTimes = 10
)

var (
	ErrClosed = errors.New("pool is closed")
	pools     []*ConnectionPool
)

type pConn struct {
	net.Conn
	pool *ConnectionPool
}

type Host struct {
	host  string
	alive bool
}

func (c pConn) Close() error {
	return c.pool.put(c.Conn)
}

type ConnectionPool struct {
	hosts    []*Host
	port     int
	minConns int
	maxConns int
	conns    chan net.Conn

	curConns int32
	l        sync.Mutex
	closed   bool
}

func ReleaseConnections() {
	for _, pool := range pools {
		pool.Close()
	}
}

func NewConnectionPool(hosts []string, port int, minConns int, maxConns int) (*ConnectionPool, error) {
	if minConns < 0 || maxConns <= 0 || minConns > maxConns {
		return nil, errors.New("invalid conns settings")
	}

	cp := &ConnectionPool{
		hosts:    make([]*Host, len(hosts)),
		port:     port,
		minConns: minConns,
		maxConns: maxConns,
		l:        sync.Mutex{},
		conns:    make(chan net.Conn, maxConns),
	}

	for i :=0; i < len(hosts); i++ {
		cp.hosts[i] = &Host{hosts[i], true}
	}

	for i := 0; i < minConns; i++ {
		conn, err := cp.makeConn()
		if err != nil {
			cp.Close()
			return nil, err
		}
		cp.conns <- conn
	}

	go cp.StartHealthCheck()
	pools = append(pools, cp)
	return cp, nil
}

func (this *ConnectionPool) StartHealthCheck() {
	var (
		addr string
		conn net.Conn
		err  error
	)
	for !this.closed {
		for _, host := range this.hosts {
			if !host.alive {
				addr = fmt.Sprintf("%s:%d", host, this.port)
				if conn, err = net.DialTimeout("tcp", addr, 10 * time.Second); err == nil {
					fmt.Printf("%s alived\n", addr)
					host.alive = true
					conn.Close()
				}
			}
		}

		time.Sleep(10 * time.Second)
	}
}

func (this *ConnectionPool) Get() (net.Conn, error) {
	conns := this.getConns()
	if conns == nil {
		return nil, ErrClosed
	}

	for {
		select {
		case conn := <-conns:
			if conn == nil {
				break
			}
			if err := this.activeConn(conn); err != nil {
				break
			}
			return this.wrapConn(conn), nil
		default:
			if int(this.curConns) >= this.maxConns {
				continue
			}

			this.l.Lock()
			defer this.l.Unlock()

			if int(this.curConns) >= this.maxConns {
				continue
			}

			conn, err := this.makeConn()
			if err != nil {
				return nil, err
			}
			return this.wrapConn(conn), nil
		}
	}
}

func (this *ConnectionPool) Close() {
	conns := this.conns
	this.conns = nil
	this.closed = true

	if conns == nil {
		return
	}

	close(conns)
	for conn := range conns {
		conn.Close()
	}
}

func (this *ConnectionPool) makeConn() (conn net.Conn, err error) {
	aliveHosts := make([]*Host, 0, len(this.hosts))
	for _, host := range this.hosts {
		if host.alive {
			aliveHosts = append(aliveHosts, host)
		}
	}

	if len(aliveHosts) == 0 {
		return nil, errors.New("no alive hosts")
	}

	host := aliveHosts[rand.Intn(len(aliveHosts))]
	if conn, err = this.makeConnInternal(host.host); err != nil {
		host.alive = false
		return this.makeConn()
	}
	return
}

func  (this *ConnectionPool) makeConnInternal(host string) (conn net.Conn, err error) {
	addr := fmt.Sprintf("%s:%d", host, this.port)

	for retry := 0; retry < DialRetryTimes; retry++ {
		if conn, err = net.DialTimeout("tcp", addr, 15 * time.Second); err == nil {
			atomic.AddInt32(&this.curConns, 1)
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	return
}


func (this *ConnectionPool) getConns() chan net.Conn {
	conns := this.conns
	return conns
}

func (this *ConnectionPool) put(conn net.Conn) error {
	if conn == nil {
		return errors.New("connection is nil")
	}
	if this.conns == nil {
		return conn.Close()
	}

	select {
	case this.conns <- conn:
		return nil
	default:
		atomic.AddInt32(&this.curConns, -1)
		return conn.Close()
	}
}

func (this *ConnectionPool) wrapConn(conn net.Conn) net.Conn {
	c := pConn{pool: this}
	c.Conn = conn
	return c
}

func (this *ConnectionPool) activeConn(conn net.Conn) error {
	th := &trackerHeader{}
	th.cmd = FDFS_PROTO_CMD_ACTIVE_TEST
	th.sendHeader(conn)
	th.recvHeader(conn)
	if th.cmd == 100 && th.status == 0 {
		return nil
	}
	return errors.New("Conn unavailable")
}

func TcpSendData(conn net.Conn, bytesStream []byte) error {
	if _, err := conn.Write(bytesStream); err != nil {
		return err
	}
	return nil
}

func TcpSendFile(conn net.Conn, filename string) error {
	file, err := os.Open(filename)
	defer file.Close()
	if err != nil {
		return err
	}

	var fileSize int64 = 0
	if fileInfo, err := file.Stat(); err == nil {
		fileSize = fileInfo.Size()
	}

	if fileSize == 0 {
		errmsg := fmt.Sprintf("file size is zeor [%s]", filename)
		return errors.New(errmsg)
	}

	fileBuffer := make([]byte, fileSize)

	_, err = file.Read(fileBuffer)
	if err != nil {
		return err
	}

	return TcpSendData(conn, fileBuffer)
}

func TcpRecvResponse(conn net.Conn, bufferSize int64) ([]byte, int64, error) {
	tmp := make([]byte, 256)
	recvBuff := make([]byte, 0, bufferSize)
	var (
		total int64
		retry int
	)
	for {
		n, err := conn.Read(tmp)
		if err != nil {
			if err != io.EOF {
				if retry < ReceiveRetryTimes {
					retry++

					time.Sleep(300 * time.Millisecond)
					continue
				}
				return nil, 0, err
			}
			break
		}

		total += int64(n)
		recvBuff = append(recvBuff, tmp[:n]...)
		if total == bufferSize {
			break
		}
	}
	return recvBuff, total, nil
}

func TcpRecvFile(conn net.Conn, localFilename string, bufferSize int64) (int64, error) {
	file, err := os.Create(localFilename)
	defer file.Close()
	if err != nil {
		return 0, err
	}

	recvBuff, total, err := TcpRecvResponse(conn, bufferSize)
	if _, err := file.Write(recvBuff); err != nil {
		return 0, err
	}
	return total, nil
}
