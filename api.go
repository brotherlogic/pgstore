package main

import (
	"context"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pstore "github.com/brotherlogic/pstore/proto"
)

func (s *Server) Read(ctx context.Context, req *pstore.ReadRequest) (*pstore.ReadResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "Not implemented")
}

func (s *Server) Write(ctx context.Context, req *pstore.WriteRequest) (*pstore.WriteResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "Not implemented")
}

func (s *Server) GetKeys(ctx context.Context, req *pstore.GetKeysRequest) (*pstore.GetKeysResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "Not implemented")
}

func (s *Server) Delete(ctx context.Context, req *pstore.DeleteRequest) (*pstore.DeleteResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "Not implemented")
}

func (s *Server) Count(ctx context.Context, req *pstore.CountRequest) (*pstore.CountResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "Not implemented")
}
