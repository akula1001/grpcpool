package grpcpool

import (
	"context"
	"net"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"

	"github.com/stretchr/testify/assert"
)

func TestShouldPutConnectionsInIdle(t *testing.T) {
	pool := ConnectionPool{}
	connection := &grpc.ClientConn{}
	pool.put(connection)

	item := pool.idle.Front()
	assert.Equal(t, connection, item.Value.(*grpc.ClientConn))
}

func TestShouldgetFirstIdleConnection(t *testing.T) {
	pool := ConnectionPool{}
	connection := &grpc.ClientConn{}
	pool.put(connection)
	idleConn := pool.idle.Front().Value.(*grpc.ClientConn)
	conn, _ := pool.get()
	assert.Equal(t, idleConn, conn)
}

func TestShouldDialNewConnection(t *testing.T) {
	dialed := false
	pool, _ := NewConnectionPool(1, 1, func() (*grpc.ClientConn, error) {
		dialed = true
		return nil, nil
	})
	pool.get()
	assert.True(t, dialed)
}

func TestShouldBlockIfNoConnectionsAreAvailable(t *testing.T) {
	blockedChannel := make(chan Connection, 1)
	go func() {
		pool, _ := NewConnectionPool(0, 1, func() (*grpc.ClientConn, error) { return nil, nil })
		conn, _ := pool.Get()
		blockedChannel <- conn
	}()

	timeoutValue := 1 * time.Second

	timeout := make(chan bool, 1)
	go func() {
		time.Sleep(timeoutValue)
		timeout <- true
	}()

	select {
	case <-blockedChannel:
		assert.Fail(t, "should have blocked")
	case <-timeout:
	}
}

func TestShouldGetUnBlockedIfConnectionsBecomeAvailable(t *testing.T) {
	blockedChannel := make(chan Connection, 1)
	pool := ConnectionPool{}

	go func() {
		conn, _ := pool.Get()
		blockedChannel <- conn
	}()

	timeout1Value := 1 * time.Second
	timeout2Value := 2 * time.Second

	timeout1 := make(chan bool, 1)
	go func() {
		time.Sleep(timeout1Value)
		pool.put(&grpc.ClientConn{})
		timeout1 <- true
	}()

	timeout2 := make(chan bool, 1)
	go func() {
		time.Sleep(timeout2Value)
		timeout2 <- true
	}()

	select {
	case <-blockedChannel:
	case <-timeout1:
	case <-timeout2:
		assert.Fail(t, "should not have blocked this long")
	}
}

func TestShouldProvideExclusiveConnection(t *testing.T) {
	pool := ConnectionPool{}

	numberOfConceurrentClients := 10
	uniqueConnections := make([]*grpc.ClientConn, numberOfConceurrentClients)
	for i := 1; i <= numberOfConceurrentClients; i++ {
		connection := new(grpc.ClientConn)
		pool.put(connection)
	}

	for i := 1; i <= numberOfConceurrentClients; i++ {
		out := make(chan *grpc.ClientConn)
		go func() {
			conn, _ := pool.Get()
			out <- conn.Get()
		}()
		pooledConn, _ := <-out
		assert.False(t, Any(uniqueConnections, func(conn *grpc.ClientConn) bool {
			return pooledConn == conn
		}))
		uniqueConnections[i-1] = pooledConn
	}
}

func TestPoolShouldNotExceedConfiguredMaxConnections(t *testing.T) {
	maxCount := 3
	created := 0
	pool, _ := NewConnectionPool(maxCount, 1, func() (*grpc.ClientConn, error) {
		created = created + 1
		mockConnection := &MockConnection{}
		mockConnection.On("Close").Return(nil)
		conn, _ := grpc.DialContext(context.Background(), "fakeaddr", grpc.WithDialer(func(string, time.Duration) (net.Conn, error) {
			return mockConnection, nil
		}), grpc.WithInsecure())
		return conn, nil
	})

	n := 1000
	wg := sync.WaitGroup{}
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func() {
			defer wg.Done()
			conn, _ := pool.Get()
			defer conn.Close()
		}()
	}
	wg.Wait()
	assert.Equal(t, maxCount, created)
}

func TestPoolShouldMakeNewConnectionsAfterEvictions(t *testing.T) {
	maxCount := 3
	created := 0
	pool, _ := NewConnectionPool(maxCount, 1, func() (*grpc.ClientConn, error) {
		created = created + 1
		mockConnection := &MockConnection{}
		mockConnection.On("Close").Return(nil)
		conn, _ := grpc.DialContext(context.Background(), "fakeaddr", grpc.WithDialer(func(string, time.Duration) (net.Conn, error) {
			return mockConnection, nil
		}), grpc.WithInsecure())
		return conn, nil
	})

	n := 100
	wg := sync.WaitGroup{}
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func() {
			defer wg.Done()
			conn, _ := pool.Get()
			conn.Evict()
		}()
	}
	wg.Wait()
	assert.True(t, created >= maxCount && created < maxCount+n)

	pool.Get()
	assert.True(t, created >= maxCount && created < maxCount+n)
}

func Any(conns []*grpc.ClientConn, f func(*grpc.ClientConn) bool) bool {
	for _, conn := range conns {
		if f(conn) {
			return true
		}
	}
	return false
}
