package main

import (
	"database/sql"
	"fmt"
	"log"
	"os"
	"time"

	pg "github.com/lib/pq"
)

func createVersionTable() error {
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
		return err
	}
	defer db.Close()

	_, err = db.Exec("CREATE TABLE IF NOT EXISTS version (version text)")
	if err != nil {
		return err
	}

	_, err = db.Exec("INSERT INTO version (version) VALUES ('1'))")

	return err
}

func checkDBVersion() (string, error) {
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
		return "", err
	}
	defer db.Close()

	// Validate that we can reach the db
	pingErr := db.Ping()
	if pingErr != nil {
		return "", pingErr
	}

	// Check the version table
	rows, err := db.Query("SELECT * FROM version")
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

	return version, nil
}

func main() {
	for {
		version, err := checkDBVersion()
		log.Printf("%v -> %v", version, err)

		if err.(*pg.Error).Code == "42P01" {
			err = createVersionTable()
			log.Printf("Created version table: %v", err)
		}

		time.Sleep(time.Minute)
	}
}
