package main

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/anypb"

	pstore "github.com/brotherlogic/pstore/proto"
)

var (
	latencyHistogram = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "pgstore_latency",
		Help:    "The latency of the requests",
		Buckets: prometheus.ExponentialBuckets(0.001, 2, 15),
	}, []string{"method"})

	requestCounter = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "pgstore_requests",
		Help: "The number of requests",
	}, []string{"method", "status"})
)

func (s *Server) Read(ctx context.Context, req *pstore.ReadRequest) (*pstore.ReadResponse, error) {
	t := time.Now()
	var err error
	defer func() {
		latencyHistogram.WithLabelValues("Read").Observe(time.Since(t).Seconds())
		requestCounter.WithLabelValues("Read", status.Code(err).String()).Inc()
	}()

	var data []byte
	err = s.db.QueryRowContext(ctx, "SELECT value FROM pgstore WHERE key = $1", req.GetKey()).Scan(&data)
	if err != nil {
		if err == sql.ErrNoRows {
			err = status.Errorf(codes.NotFound, "%v was not found in the db", req.GetKey())
			return nil, err
		}
		return nil, err
	}

	return &pstore.ReadResponse{Value: &anypb.Any{Value: data}}, nil
}

func (s *Server) Write(ctx context.Context, req *pstore.WriteRequest) (*pstore.WriteResponse, error) {
	t := time.Now()
	var err error
	defer func() {
		latencyHistogram.WithLabelValues("Write").Observe(time.Since(t).Seconds())
		requestCounter.WithLabelValues("Write", status.Code(err).String()).Inc()
	}()

	_, err = s.db.ExecContext(ctx, "INSERT INTO pgstore (key, value) VALUES ($1, $2) ON CONFLICT (key) DO UPDATE SET value = $2", req.Key, req.Value.Value)
	return &pstore.WriteResponse{}, err
}

func (s *Server) GetKeys(ctx context.Context, req *pstore.GetKeysRequest) (*pstore.GetKeysResponse, error) {
	t := time.Now()
	var err error
	defer func() {
		latencyHistogram.WithLabelValues("GetKeys").Observe(time.Since(t).Seconds())
		requestCounter.WithLabelValues("GetKeys", status.Code(err).String()).Inc()
	}()

	query := "SELECT key FROM pgstore WHERE 1=1"
	var args []interface{}

	if req.GetPrefix() != "" {
		query += fmt.Sprintf(" AND key LIKE $%d", len(args)+1)
		args = append(args, req.GetPrefix()+"%")
	}

	for _, suffix := range req.GetAvoidSuffix() {
		query += fmt.Sprintf(" AND key NOT LIKE $%d", len(args)+1)
		args = append(args, "%"+suffix)
	}

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("Query error: %w", err)
	}
	defer rows.Close()

	var keys []string
	for rows.Next() {
		var key string
		if err := rows.Scan(&key); err == nil {
			keys = append(keys, key)
		}
	}

	return &pstore.GetKeysResponse{Keys: keys}, nil
}

func (s *Server) Delete(ctx context.Context, req *pstore.DeleteRequest) (*pstore.DeleteResponse, error) {
	t := time.Now()
	var err error
	defer func() {
		latencyHistogram.WithLabelValues("Delete").Observe(time.Since(t).Seconds())
		requestCounter.WithLabelValues("Delete", status.Code(err).String()).Inc()
	}()

	_, err = s.db.ExecContext(ctx, "DELETE FROM pgstore WHERE key = $1", req.GetKey())
	return &pstore.DeleteResponse{}, err
}

func (s *Server) Count(ctx context.Context, req *pstore.CountRequest) (*pstore.CountResponse, error) {
	t := time.Now()
	var err error
	defer func() {
		latencyHistogram.WithLabelValues("Count").Observe(time.Since(t).Seconds())
		requestCounter.WithLabelValues("Count", status.Code(err).String()).Inc()
	}()

	var value int64
	err = s.db.QueryRowContext(ctx, "INSERT INTO counters (key, value) VALUES ($1, 1) ON CONFLICT (key) DO UPDATE SET value = counters.value + 1 RETURNING value", req.GetCounter()).Scan(&value)
	if err != nil {
		return nil, fmt.Errorf("unable to count: %w", err)
	}

	return &pstore.CountResponse{Count: value}, nil
}
