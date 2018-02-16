package grpcpool

import (
	"testing"

	"google.golang.org/grpc"

	"context"
	"net"
	"time"

	"github.com/stretchr/testify/mock"
)

type MockPool struct {
	mock.Mock
	mockConnection Connection
}

func (m MockPool) Get() (Connection, error) {
	return nil, nil
}

func (m MockPool) Close() {

}

func (m MockPool) put(client *grpc.ClientConn) error {
	m.Called(client)
	return nil
}

func (m MockPool) evicted() {
}

func TestShouldPutConnectionBackInPoolAfterClose(t *testing.T) {
	mockPool := new(MockPool)

	mockConnection := &MockConnection{}
	conn, _ := grpc.DialContext(context.Background(), "fakeaddr", grpc.WithDialer(func(string, time.Duration) (net.Conn, error) {
		return mockConnection, nil
	}), grpc.WithInsecure())

	grpcConnection := &GrpcConnection{pool: mockPool, GrpcConn: conn}
	mockPool.On("put", grpcConnection.GrpcConn).Return(nil)

	grpcConnection.Close()

	mockConnection.AssertNotCalled(t, "Close")
}

func TestShouldPutEvictConnectionFromPool(t *testing.T) {
	mockPool := new(MockPool)

	mockConnection := &MockConnection{}
	conn, _ := grpc.DialContext(context.Background(), "fakeaddr", grpc.WithDialer(func(string, time.Duration) (net.Conn, error) {
		return mockConnection, nil
	}), grpc.WithInsecure())

	grpcConnection := &GrpcConnection{pool: mockPool, GrpcConn: conn}
	mockPool.On("put", grpcConnection.GrpcConn).Return(nil)

	grpcConnection.Evict()

	mockConnection.AssertExpectations(t)
	mockConnection.AssertNotCalled(t, "Close")
}
