package grpcpool

import (
	"io"

	"google.golang.org/grpc"
)

type Evictor interface {
	Evict()
}

type Connection interface {
	io.Closer
	Evictor
	Get() *grpc.ClientConn
}

type GrpcConnection struct {
	pool     Pool
	GrpcConn *grpc.ClientConn
}

func (self *GrpcConnection) Close() error {
	return self.pool.put(self.GrpcConn)
}

func (self *GrpcConnection) Get() *grpc.ClientConn {
	return self.GrpcConn
}

func (self *GrpcConnection) Evict() {
	self.GrpcConn.Close()
	self.pool.evicted()
}
