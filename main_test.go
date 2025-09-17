package main

import (
	"context"
	"database/sql"
	"testing"

	"github.com/stapelberg/postgrestest"
)

var pgt *postgrestest.Server

func TestMain(m *testing.M) {
	var err error
	pgt, err = postgrestest.Start(context.Background())
	if err != nil {
		panic(err)
	}
	defer pgt.Cleanup()

	m.Run()
}

func TestInitDB(t *testing.T) {
	pgurl, err := pgt.CreateDatabase(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("postgres", pgurl)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	s := &server{db: db}
	s.initDB()

	// Check that we've built to version2
	version, err := s.checkDBVersion()
	if err != nil {
		t.Fatal(err)
	}
	if version != "2" {
		t.Errorf("Bad db version: %v", version)
	}
}
