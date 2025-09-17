package main

import (
	"context"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/anypb"

	pstore "github.com/brotherlogic/pstore/proto"
)

func (s *Server) Read(ctx context.Context, req *pstore.ReadRequest) (*pstore.ReadResponse, error) {
	// Check the version table
	rows, err := s.db.Query("SELECT value FROM pgstore WHERE key = $1", req.GetKey())
	if err != nil {
		return nil, err
	}

	defer rows.Close()
	var data []byte
	for rows.Next() {
		if err := rows.Scan(&data); err == nil {
			return &pstore.ReadResponse{Value: &anypb.Any{Value: data}}, err
		} else {
			return nil, err
		}
	}

	return nil, status.Errorf(codes.NotFound, "%v was not found in the db", req.GetKey())
}

func (s *Server) Write(ctx context.Context, req *pstore.WriteRequest) (*pstore.WriteResponse, error) {
	_, err := s.db.Exec("INSERT INTO pgstore (key, value) VALUES ($1, $2)", req.Key, req.Value.Value)
	return &pstore.WriteResponse{}, err
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
