package main

import (
	"context"
	"database/sql"
	"fmt"
	"testing"

	pstore "github.com/brotherlogic/pstore/proto"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
)

func TestHeavyWrite(t *testing.T) {
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

	for i := 0; i < 10000; i++ {
		_, err = s.Write(context.Background(), &pstore.WriteRequest{Key: fmt.Sprintf("testing-%v", i), Value: &anypb.Any{Value: datav}})
		if err != nil {
			t.Fatalf("Unable to write: %v", err)
		}
	}

	keys, err := s.GetKeys(context.Background(), &pstore.GetKeysRequest{})
	if err != nil {
		t.Fatalf("Unable to get all keys: %v", err)
	}

	if len(keys.GetKeys()) != 10000 {
		t.Errorf("Not enough keys: %v", len(keys.GetKeys()))
	}
}

func TestDelete(t *testing.T) {
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

	_, err = s.Delete(context.Background(), &pstore.DeleteRequest{Key: "testing"})
	if err != nil {
		t.Fatalf("Unable to delete: %v", err)
	}

	keys, err := s.GetKeys(context.Background(), &pstore.GetKeysRequest{})
	if err != nil {
		t.Fatalf("Unable to get all keys: %v", err)
	}

	if len(keys.GetKeys()) != 0 {
		t.Errorf("Too many keys: %v", keys)
	}
}

func TestCounter(t *testing.T) {
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

	resp, err := s.Count(context.Background(), &pstore.CountRequest{Counter: "testing"})
	if err != nil {
		t.Fatalf("Unable to count: %v", err)
	}

	if resp.GetCount() != 1 {
		t.Fatalf("Did not get right counter: %v", resp)
	}

	resp, err = s.Count(context.Background(), &pstore.CountRequest{Counter: "testing"})
	if err != nil {
		t.Fatalf("Unable to count: %v", err)
	}

	if resp.GetCount() != 2 {
		t.Fatalf("Did not get right counter: %v", resp)
	}
}

func TestReadDualWrite(t *testing.T) {
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
		t.Errorf("Read has come back and is wrong: %v", ndata)
	}

	// Let's overwrite the key
	data = &pstore.ReadRequest{Key: "hello2"}
	datav, err = proto.Marshal(data)
	if err != nil {
		t.Fatalf("Cannot marshal: %v", err)
	}
	_, err = s.Write(context.Background(), &pstore.WriteRequest{Key: "testing", Value: &anypb.Any{Value: datav}})
	if err != nil {
		t.Fatalf("Unable to write: %v", err)
	}

	val, err = s.Read(context.Background(), &pstore.ReadRequest{Key: "testing"})
	if err != nil {
		t.Fatalf("Unable to read: %v", err)
	}

	ndata = &pstore.ReadRequest{}
	err = proto.Unmarshal(val.GetValue().GetValue(), ndata)
	if err != nil {
		t.Fatalf("Unable to unmarshal: %v", err)
	}
	if ndata.Key != "hello2" {
		t.Errorf("Read has come back and is wrong: %v", ndata)
	}

}
