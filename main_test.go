package main

import (
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
)

func createTestServer() *server {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
	}
	defer db.Close()

}

func TestInitDB(t *testing.T) {

}
