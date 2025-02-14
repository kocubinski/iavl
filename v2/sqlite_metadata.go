package iavl

import (
	dsql "database/sql"
	"errors"
	"fmt"
	"os"
)

// SqliteKVStore is a generic KV store which uses sqlite as the backend and be used by applications to store and
// retrieve generic key-value pairs, probably for metadata.
type SqliteKVStore struct {
	options SqliteDbOptions
	db      *dsql.DB
}

func NewSqliteKVStore(opts SqliteDbOptions) (kv *SqliteKVStore, err error) {
	if opts.Path == "" {
		return nil, errors.New("path cannot be empty")
	}
	if opts.WalSize == 0 {
		opts.WalSize = 50 * 1024 * 1024
	}

	pageSize := os.Getpagesize()
	kv = &SqliteKVStore{options: opts}
	kv.db, err = dsql.Open(driverName, fmt.Sprintf(
		"file:%s?_journal_mode=WAL&_synchronous=OFF&&_wal_autocheckpoint=%d", opts.Path, pageSize/opts.WalSize))
	if err != nil {
		return nil, err
	}

	// Create the tables if they don't exist
	if _, err = kv.db.Exec("CREATE TABLE IF NOT EXISTS kv (key BLOB PRIMARY KEY, value BLOB)"); err != nil {
		return nil, err
	}

	return kv, nil
}

func (kv *SqliteKVStore) Set(key []byte, value []byte) error {
	_, err := kv.db.Exec("INSERT OR REPLACE INTO kv (key, value) VALUES (?, ?)", key, value)
	return err
}

func (kv *SqliteKVStore) Get(key []byte) (value []byte, err error) {
	row := kv.db.QueryRow("SELECT value FROM kv WHERE key = ?")
	err = row.Scan(&value)
	if errors.Is(err, dsql.ErrNoRows) {
		return nil, nil
	} else if err != nil {
		return nil, err
	}
	return value, nil
}

func (kv *SqliteKVStore) Delete(key []byte) error {
	_, err := kv.db.Exec("DELETE FROM kv WHERE key = ?", key)
	return err
}
