package datastore

import (
	"bytes"
	"context"
	"os"
	"testing"
)

func getDatastore() *Datastore {
	os.Mkdir("test", 0777)
	os.Remove("test/testdb.db")
	ds := NewDatastore("test/testdb.db", "keystore")
	ds.Connect()
	return ds
}

func clearDatastore(ds *Datastore) {
	ds.db.Close()
	os.RemoveAll("test")
}

func TestDatastore(t *testing.T) {
	ds := getDatastore()
	defer clearDatastore(ds)

	key := "testkey"
	value := []byte("testvalue")

	has, err := ds.Has(context.Background(), key)
	if err != nil {
		t.Errorf("Expected no error but got %s", err)
	}
	if has {
		t.Errorf("Expected has=false but got has=%t", has)
	}

	err = ds.Put(context.Background(), key, value)
	if err != nil {
		t.Errorf("Expected no error but got %s", err)
	}

	has, err = ds.Has(context.Background(), key)
	if err != nil {
		t.Errorf("Expected no error but got %s", err)
	}
	if !has {
		t.Errorf("Expected has=true but got has=%t", has)
	}

	content, err := ds.Get(context.Background(), key)
	if err != nil {
		t.Errorf("Expected no error but got %s", err)
	}

	same := bytes.Compare(content, value)
	if same != 0 {
		t.Errorf("Expected same %s=%s", content, value)
	}
}
