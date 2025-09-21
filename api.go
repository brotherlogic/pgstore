package main

import (
	"context"
	"fmt"
	"log"
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
	log.Printf("Running Write %v", req.GetKey())
	defer log.Printf("Completed write %v", req.GetKey())
	_, err := s.db.Exec("INSERT INTO pgstore (key, value) VALUES ($1, $2) ON CONFLICT (key) DO UPDATE SET value = $2", req.Key, req.Value.Value)
	if err != nil {
		// Dump the connection table:
		rows, nerr := s.db.Query("SELECT count(*) FROM pg_stat_activity")
		if nerr != nil {
			return nil, err
		}
		defer rows.Close()
		var query int
		for rows.Next() {
			serr := rows.Scan(&query)
			log.Printf("%v from %v-> %v with %v", nerr, err, query, serr)
		}
	}
	return &pstore.WriteResponse{}, err
}

func (s *Server) GetKeys(ctx context.Context, req *pstore.GetKeysRequest) (*pstore.GetKeysResponse, error) {
	if req.GetPrefix() != "" {
		rows, err := s.db.Query("SELECT key FROM pgstore WHERE key LIKE $1", req.GetPrefix()+"%")
		if err != nil {
			return nil, err
		}
		defer rows.Close()

		var key string
		var keys []string
		for rows.Next() {
			if err := rows.Scan(&key); err == nil {
				keys = append(keys, key)
			}
		}

		return &pstore.GetKeysResponse{Keys: keys}, nil
	}

	if req.GetPrefix() == "" {
		rows, err := s.db.Query("SELECT key FROM pgstore")
		if err != nil {
			return nil, err
		}
		defer rows.Close()

		var key string
		var keys []string
		for rows.Next() {
			if err := rows.Scan(&key); err == nil {
				keys = append(keys, key)
			}
		}

		return &pstore.GetKeysResponse{Keys: keys}, nil
	}
	return nil, status.Errorf(codes.Unimplemented, "Not implemented")
}

func (s *Server) Delete(ctx context.Context, req *pstore.DeleteRequest) (*pstore.DeleteResponse, error) {
	_, err := s.db.Exec("DELETE FROM pgstore WHERE key = $1", req.GetKey())
	return &pstore.DeleteResponse{}, err
}

func (s *Server) Count(ctx context.Context, req *pstore.CountRequest) (*pstore.CountResponse, error) {
	_, err := s.db.Exec("UPDATE counters SET value = value + 1 WHERE key = $1", req.GetCounter())
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
	_, err = s.db.Exec("INSERT INTO counters VALUES ($1, 1)", req.GetCounter())
	return &pstore.CountResponse{Count: 1}, err
}
