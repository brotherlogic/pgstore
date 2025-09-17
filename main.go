package main

import (
	"database/sql"
	"fmt"
	"log"
	"os"
	"time"

	pg "github.com/lib/pq"
)

type server struct {
	db *sql.DB
}

func createServer() (*server, error) {
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

	return &server{db: db}, nil
}

func (s *server) createStorageTable() error {
	_, err := s.db.Exec("CREATE TABLE IF NOT EXISTS pgstore (key VARCHAR(100) PRIMARY KEY, value BYTEA)")
	return err
}

func (s *server) createVersionTable(value int) error {
	_, err := s.db.Exec("CREATE TABLE IF NOT EXISTS version (version text)")
	if err != nil {
		return fmt.Errorf("bad create: %w", err)
	}

	_, err = s.db.Exec("INSERT INTO version VALUES ($1)", value)

	return err
}

func (s *server) checkDBVersion() (string, error) {
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

func (s *server) updateVersion(val int) error {
	_, err := s.db.Exec("UPDATE version SET version = $1", val)
	return err
}

func (s *server) initDB() error {
	// Inits the DB to version 2
	version, err := s.checkDBVersion()

	if err != nil && err.(*pg.Error).Code == "42P01" {
		err = s.createVersionTable(2)
		if err != nil {
			return fmt.Errorf("unable to create version table: %w", err)
		}
		err = s.createStorageTable()
		if err != nil {
			return fmt.Errorf("unable to create storage table: %w", err)
		}
		err = s.createStorageTable()
		if err != nil {
			return err
		}

		return nil
	}

	if version == "1" {
		err = s.createStorageTable()
		if err != nil {
			return err
		}
		err = s.updateVersion(2)
		if err != nil {
			return err
		}
	}

	if version == "2" {
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

	for {
		log.Printf("Serving")
		time.Sleep(time.Minute)
	}
}
