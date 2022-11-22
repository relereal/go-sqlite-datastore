package datastore

import (
	"context"
	"database/sql"
	"fmt"
	"os"

	_ "github.com/mattn/go-sqlite3"
)

type Datastore struct {
	db        *sql.DB
	dbPath    string
	tableName string
}

func NewDatastore(dbPath string, tableName string) *Datastore {
	// Create database if not already present
	if _, err := os.Stat(dbPath); err != nil {
		file, err := os.Create(dbPath)
		if err != nil {
			panic(err)
		}
		file.Close()
	}
	return &Datastore{
		dbPath:    dbPath,
		tableName: tableName,
	}
}

func (ds *Datastore) Connect() {
	// Open connection to the database
	db, err := sql.Open("sqlite3", ds.dbPath)
	if err != nil {
		panic(err)
	}

	// Create table acting as key-value store (string: blob)
	createTableQuery := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (key TEXT NOT NULL PRIMARY KEY, value BLOB)", ds.tableName)
	_, err = db.Exec(createTableQuery)
	if err != nil {
		panic(err)
	}

	createIndexQuery := fmt.Sprintf("CREATE INDEX IF NOT EXISTS key_index ON %s (key)", ds.tableName)
	_, err = db.Exec(createIndexQuery)
	if err != nil {
		panic(err)
	}

	// Save db connection
	ds.db = db
}

func (ds *Datastore) Has(ctx context.Context, key string) (bool, error) {
	query := fmt.Sprintf("SELECT key FROM %s WHERE key = ?", ds.tableName)
	keys, err := ds.db.Query(query, key)
	if err != nil {
		return false, err
	}
	defer keys.Close()

	if keys.Next() {
		return true, nil
	}
	return false, nil
}

func (ds *Datastore) Get(ctx context.Context, key string) ([]byte, error) {
	query := fmt.Sprintf("SELECT value FROM %s WHERE key = ?", ds.tableName)
	var value []byte
	if err := ds.db.QueryRow(query, key).Scan(&value); err != nil {
		return nil, err
	}
	return value, nil
}

func (ds *Datastore) Put(ctx context.Context, key string, content []byte) error {
	// Check if key already in datastore, if so, we do not need to do anything.
	// In our system, key is a cid of the content, so each key will be always associated with the same content
	has, err := ds.Has(ctx, key)
	if err != nil {
		return err
	}
	if has {
		return nil
	}

	// If key not already present, add it
	query := fmt.Sprintf("INSERT INTO %s (key, value) VALUES(?, ?)", ds.tableName)
	_, err = ds.db.Exec(query, key, content)
	if err != nil {
		return err
	}
	return nil
}
