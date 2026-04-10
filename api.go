package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"strings"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/anypb"

	pstore "github.com/brotherlogic/pstore/proto"
)

func (s *Server) Read(ctx context.Context, req *pstore.ReadRequest) (*pstore.ReadResponse, error) {
	t := time.Now()
	defer func() {
		log.Printf("Read %v took %v", req.GetKey(), time.Since(t))
	}()
	// Check the version table
	rows, err := s.db.Query("SELECT value FROM pgstore WHERE key = $1", req.GetKey())
	if err != nil {
		log.Printf("Error in queury: %v", err)
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
	_, err := s.db.ExecContext(ctx, "INSERT INTO pgstore (key, value) VALUES ($1, $2) ON CONFLICT (key) DO UPDATE SET value = $2", req.Key, req.Value.Value)
	return &pstore.WriteResponse{}, err
}

func (s *Server) GetKeys(ctx context.Context, req *pstore.GetKeysRequest) (*pstore.GetKeysResponse, error) {
	var rows *sql.Rows
	var err error
	if req.GetPrefix() != "" {
		rows, err = s.db.Query("SELECT key FROM pgstore WHERE key LIKE $1", req.GetPrefix()+"%")
	} else {
		rows, err = s.db.Query("SELECT key FROM pgstore")
	}

	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var keys []string
	for rows.Next() {
		var key string
		if err := rows.Scan(&key); err == nil {
			avoid := false
			for _, suffix := range req.GetAvoidSuffix() {
				if strings.HasSuffix(key, suffix) {
					avoid = true
					break
				}
			}
			if !avoid {
				keys = append(keys, key)
			}
		}
	}

	return &pstore.GetKeysResponse{Keys: keys}, nil
}

func (s *Server) Delete(ctx context.Context, req *pstore.DeleteRequest) (*pstore.DeleteResponse, error) {
	_, err := s.db.ExecContext(ctx, "DELETE FROM pgstore WHERE key = $1", req.GetKey())
	return &pstore.DeleteResponse{}, err
}

func (s *Server) Count(ctx context.Context, req *pstore.CountRequest) (*pstore.CountResponse, error) {
	_, err := s.db.ExecContext(ctx, "UPDATE counters SET value = value + 1 WHERE key = $1", req.GetCounter())
	if err != nil {
		return nil, fmt.Errorf("unable to update: %w", err)
	}

	rows, err := s.db.Query("SELECT value FROM counters WHERE key = $1", req.GetCounter())
	if err != nil {
		return nil, fmt.Errorf("unable to select: %w", err)
	}
	defer rows.Close()

	var value int64
	for rows.Next() {
		if err := rows.Scan(&value); err == nil {
			return &pstore.CountResponse{Count: value}, nil
		}
	}

	// We need to do an insert here
	_, err = s.db.ExecContext(ctx, "INSERT INTO counters VALUES ($1, 1)", req.GetCounter())
	return &pstore.CountResponse{Count: 1}, err
}
