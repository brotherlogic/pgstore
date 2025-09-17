package main

import (
	"database/sql"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"

	pg "github.com/lib/pq"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"google.golang.org/grpc"

	pstore "github.com/brotherlogic/pstore/proto"
)

var (
	port        = flag.Int("port", 8080, "The server port.")
	metricsPort = flag.Int("metrics_port", 8081, "Metrics port")
)

type Server struct {
	db *sql.DB
}

func createServer() (*Server, error) {
	psqlInfo := fmt.Sprintf(
		"host=%s port=%s user=%s password=%s dbname=%s sslmode=disable",
		os.Getenv("PG_HOST"),
		os.Getenv("PG_PORT"),
		os.Getenv("PG_USER"),
		os.Getenv("PG_PASSWORD"),
		os.Getenv("PG_DBNAME"))
	log.Printf("OPENING %v", psqlInfo)
	db, err := sql.Open("postgres", psqlInfo)
	if err != nil {
		return nil, err
	}

	return &Server{db: db}, nil
}

func (s *Server) createStorageTable() error {
	_, err := s.db.Exec("CREATE TABLE IF NOT EXISTS pgstore (key VARCHAR(100) PRIMARY KEY, value BYTEA)")
	return err
}

func (s *Server) createCounterTable() error {
	_, err := s.db.Exec("CREATE TABLE IF NOT EXISTS counters (key VARCHAR(100) PRIMARY KEY, value INT)")
	return err
}

func (s *Server) createVersionTable(value int) error {
	_, err := s.db.Exec("CREATE TABLE IF NOT EXISTS version (version text)")
	if err != nil {
		return fmt.Errorf("bad create: %w", err)
	}

	_, err = s.db.Exec("INSERT INTO version VALUES ($1)", value)

	return err
}

func (s *Server) checkDBVersion() (string, error) {
	// Validate that we can reach the db
	pingErr := s.db.Ping()
	if pingErr != nil {
		return "", pingErr
	}

	// Check the version table
	rows, err := s.db.Query("SELECT * FROM version")
	if err != nil {
		return "", err
	}

	defer rows.Close()
	var version string

	count := 0
	for rows.Next() {
		if err := rows.Scan(&version); err != nil {
			return "", err
		}
		count++
	}

	if count > 1 {
		return "", fmt.Errorf("mulitple rows in the version table")
	}
	if count == 0 {
		return "", fmt.Errorf("no rows found")
	}

	return version, nil
}

func (s *Server) updateVersion(val int) error {
	_, err := s.db.Exec("UPDATE version SET version = $1", val)
	return err
}

func (s *Server) initDB() error {
	// Inits the DB to version 2
	version, err := s.checkDBVersion()

	if err != nil && err.(*pg.Error).Code == "42P01" {
		err = s.createVersionTable(1)
		if err != nil {
			return fmt.Errorf("unable to create version table: %w", err)
		}
		version = "1"
	}

	// Version 2 adds the first version of the storage table
	if version == "1" {
		err = s.createStorageTable()
		if err != nil {
			return err
		}
		err = s.updateVersion(2)
		if err != nil {
			return err
		}
		version = "2"
	}

	// Version 3 adds the counter table
	if version == "2" {
		err = s.createCounterTable()
		if err != nil {
			return err
		}
		err = s.updateVersion(3)
		if err != nil {
			return err
		}
		version = "3"
	}

	if version == "3" {
		return nil
	}

	return fmt.Errorf("bad version response: %v, %v", version, err)
}

func main() {
	server, err := createServer()
	if err != nil {
		log.Fatalf("unable to create server: %v", err)
	}

	err = server.initDB()
	if err != nil {
		log.Fatalf("unable to init the db: %v", err)
	}

	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", *port))
	if err != nil {
		log.Fatalf("rstore failed to listen on the serving port %v: %v", *port, err)
	}
	size := 1024 * 1024 * 1000
	gs := grpc.NewServer(
		grpc.MaxSendMsgSize(size),
		grpc.MaxRecvMsgSize(size),
	)
	pstore.RegisterPStoreServiceServer(gs, server)
	log.Printf("rstore is listening on %v", lis.Addr())

	// Setup prometheus export
	http.Handle("/metrics", promhttp.Handler())
	go func() {
		http.ListenAndServe(fmt.Sprintf(":%v", *metricsPort), nil)
	}()

	if err := gs.Serve(lis); err != nil {
		log.Fatalf("rstore failed to serve: %v", err)
	}
}
