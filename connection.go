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
	DialRetryTimes      = 10
	ReceiveRetryTimes   = 10
	MaxFailCount        = 3
	HealthCheckInterval = 5 * time.Second
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
	host      string
	alive     bool
	failCount int
	curConns  int32
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
		cp.hosts[i] = &Host{hosts[i], true, 0, 0}
	}

	for i := 0; i < minConns; i++ {
		conn, err := cp.makeConn()
		if err != nil {
			cp.Close()
			return nil, err
		}
		cp.conns <- conn
	}

	go cp.doHealthCheck()
	pools = append(pools, cp)
	return cp, nil
}

func (this *ConnectionPool) doHealthCheck() {
	for !this.closed {
		this.check()
		time.Sleep(HealthCheckInterval)
	}
}

func (this *ConnectionPool) check() {
	var (
		err         error
		addr        string
		conn        net.Conn
		originAlive bool
	)

	for _, host := range this.hosts {
		originAlive = host.alive
		addr = this.getFullAddr(host)
		if conn, err = net.DialTimeout("tcp", addr, 1 * time.Second); err == nil {
			host.failCount = 0
			host.alive = true
			conn.Close()
		} else {
			host.failCount += 1
			host.alive = host.failCount < MaxFailCount
		}

		if originAlive != host.alive {
			logger.Warnf("Host `%s` status changed, %v -> %v", addr, originAlive, host.alive)
		}
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
				this.wrapClose(conn)
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
		this.wrapClose(conn)
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
		return nil, errors.New("No available hosts")
	}

	host := aliveHosts[rand.Intn(len(aliveHosts))]
	if conn, err = this.makeConnInternal(host.host); err != nil {
		logger.Warnf("Host `%s:%d` is dead\n", host.host, this.port)

		host.alive = false
		return this.makeConn()
	} else {
		atomic.AddInt32(&this.curConns, 1)
		atomic.AddInt32(&host.curConns, 1)
	}
	return
}

func  (this *ConnectionPool) makeConnInternal(host string) (conn net.Conn, err error) {
	addr := fmt.Sprintf("%s:%d", host, this.port)

	for retry := 0; retry < DialRetryTimes; retry++ {
		if conn, err = net.DialTimeout("tcp", addr, 15 * time.Second); err == nil {
			break
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
	
	//rebalance
	avg := int32(this.maxConns / len(this.hosts))
	if host, err := this.findHostByConn(conn); err == nil && host.curConns > avg {
		return this.wrapClose(conn)
	}

	select {
	case this.conns <- conn:
		return nil
	default:
		return this.wrapClose(conn)
	}
}

func (this *ConnectionPool) findHostByConn(conn net.Conn) (*Host, error) {
	for _, h := range this.hosts {
		if conn.RemoteAddr().String() == this.getFullAddr(h) {
			return h, nil
		}
	}
	return nil, errors.New("Unreachable code")
}

func (this *ConnectionPool) getFullAddr(host *Host) string {
	return fmt.Sprintf("%s:%d", host.host, this.port)
}

func (this *ConnectionPool) wrapClose(conn net.Conn) error {
	if h, err := this.findHostByConn(conn); err == nil {
		atomic.AddInt32(&h.curConns, -1)
	}
	atomic.AddInt32(&this.curConns, -1)
	return conn.Close()
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
