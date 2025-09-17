package main

import (
	"context"
	"database/sql"
	"testing"

	pstore "github.com/brotherlogic/pstore/proto"
	"github.com/stapelberg/postgrestest"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
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

	s := &Server{db: db}
	err = s.initDB()
	if err != nil {
		t.Fatalf("Unable to init db: %v", err)
	}

	// Check that we've built to version2
	version, err := s.checkDBVersion()
	if err != nil {
		t.Fatal(err)
	}
	if version != "2" {
		t.Errorf("Bad db version: %v", version)
	}
}

func TestReadWrite(t *testing.T) {
	pgurl, err := pgt.CreateDatabase(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("postgres", pgurl)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	s := &Server{db: db}
	err = s.initDB()
	if err != nil {
		t.Fatalf("Unable to init db: %v", err)
	}

	data := &pstore.ReadRequest{Key: "hello"}
	datav, err := proto.Marshal(data)
	if err != nil {
		t.Fatalf("Cannot marshal: %v", err)
	}

	_, err = s.Write(context.Background(), &pstore.WriteRequest{Key: "testing", Value: &anypb.Any{Value: datav}})
	if err != nil {
		t.Fatalf("Unable to write: %v", err)
	}

	val, err := s.Read(context.Background(), &pstore.ReadRequest{Key: "testing"})
	if err != nil {
		t.Fatalf("Unable to read: %v", err)
	}

	ndata := &pstore.ReadRequest{}
	err = proto.Unmarshal(val.GetValue().GetValue(), ndata)
	if err != nil {
		t.Fatalf("Unable to unmarshal: %v", err)
	}
	if ndata.Key != "hello" {
		t.Errorf("Read has come back and is wrong somehow: %v", ndata)
	}
}
