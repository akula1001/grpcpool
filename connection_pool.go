package grpcpool

import (
	"container/list"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
)

type Pool interface {
	Get() (Connection, error)
	Close()
	put(client *grpc.ClientConn) error
	evicted()
}

type DialFunc func() (*grpc.ClientConn, error)

type ConnectionPool struct {
	mu           sync.Mutex
	cond         *sync.Cond
	closed       bool
	idle         list.List
	maxCount     int
	createdCount int64
	dialFunc     DialFunc
}

func (self *ConnectionPool) Get() (Connection, error) {
	conn, err := self.get()
	if err != nil {
		// TODO : error handling
		return nil, err
	}
	return &GrpcConnection{pool: self, GrpcConn: conn}, nil
}

func (self *ConnectionPool) Close() {
	self.mu.Lock()
	idle := self.idle
	self.idle.Init()
	self.closed = true
	if self.cond != nil {
		self.cond.Broadcast()
	}
	self.mu.Unlock()
	for e := idle.Front(); e != nil; e = e.Next() {
		e.Value.(*grpc.ClientConn).Close()
	}
}

func (self *ConnectionPool) put(client *grpc.ClientConn) error {
	self.mu.Lock()
	if self.closed {
		self.mu.Unlock()
		return client.Close()
	}

	self.idle.PushFront(client)
	self.mu.Unlock()
	if self.cond != nil {
		self.cond.Broadcast()
	}
	return nil
}

func (self *ConnectionPool) get() (*grpc.ClientConn, error) {
	self.mu.Lock()
	for {
		element := self.idle.Front()
		if element != nil {
			self.idle.Remove(element)
			client := element.Value.(*grpc.ClientConn)
			self.mu.Unlock()
			return client, nil
		}

		if self.closed {
			return nil, errors.New("Pool is closed")
		}

		// wait for client to be available
		if self.cond == nil {
			self.cond = sync.NewCond(&self.mu)
		}
		self.cond.Wait()
	}
}

func (self *ConnectionPool) create() (*grpc.ClientConn, error) {
	conn, err := self.dialFunc()
	if err != nil {
		return nil, err
	}
	self.atomicAdd(1)
	self.put(conn)
	return conn, nil
}

func (self *ConnectionPool) evicted() {
	if self.atomicAdd(-1) < 0 {
		panic("pool size is negative")
	}
	// self.create()
}

func (self *ConnectionPool) atomicAdd(delta int64) int64 {
	return atomic.AddInt64(&self.createdCount, delta)
}

func NewConnectionPool(maxCount int, createIntervalInMS int, dialFunc DialFunc) (*ConnectionPool, error) {
	pool := &ConnectionPool{
		mu:       sync.Mutex{},
		maxCount: maxCount,
		dialFunc: dialFunc,
	}
	for i := 0; i < maxCount; i++ {
		_, err := pool.create()
		if err != nil {
			pool.Close()
			return nil, err
		}
	}

	go func() {
		for {
			if pool.cond != nil {
				pool.cond.Broadcast()
			}
			if pool.createdCount < int64(pool.maxCount) {
				pool.create()
			}
			time.Sleep(time.Duration(createIntervalInMS) * time.Millisecond)
		}
	}()
	return pool, nil
}
