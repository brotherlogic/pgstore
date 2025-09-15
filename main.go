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

	return "", nil
}

func main() {
	for {
		version, err := checkDBVersion()
		log.Printf("%v -> %v", version, err)

		time.Sleep(1)
	}
}
