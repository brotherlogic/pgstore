package main

import (
	"database/sql"
	"fmt"
	"log"
	"os"
	"time"

	_ "github.com/lib/pq"
)

func checkDBVersion() (string, error) {
	psqlInfo := fmt.Sprintf(
		"host=%s port=%d user=%s password=%s dbname=%s sslmode=disable",
		os.Getenv("PG_HOST"),
		os.Getenv("PG_PORT"),
		os.Getenv("PG_USER"),
		os.Getenv("PG_PASSWORD"),
		os.Getenv("PG_DBNAME"))
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

	for rows.Next() {
		if err := rows.Scan(&version); err != nil {
			return "", err
		}
	}

	return version, nil
}

func main() {
	for {
		version, err := checkDBVersion()
		log.Printf("%v -> %v", version, err)

		time.Sleep(time.Minute)
	}
}
