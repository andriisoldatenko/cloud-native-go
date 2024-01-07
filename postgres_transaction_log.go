package main

import (
	"database/sql"
	"fmt"

	_ "github.com/lib/pq"
)


type PostgresDBParams struct {
	dbName string
	host string
	user string
	password string
}

type PostgresTransactionLogger struct {
	events chan<- Event
	errors <-chan error
	db *sql.DB
}


func (l *PostgresTransactionLogger) WritePut(key, value string) {
	l.events <- Event{EventType: EventPut, Key: key, Value: value}
}

func (l *PostgresTransactionLogger) WriteDelete(key string) {
	l.events <- Event{EventType: EventDelete, Key: key}
}

func (l *PostgresTransactionLogger) Err() <-chan error {
	return l.errors
}

func (l *PostgresTransactionLogger) verifyTableExists() (bool, error) {
	return false, nil
}

func (l *PostgresTransactionLogger) createTable() error {
	return nil
}


func (l *PostgresTransactionLogger) Run() {

	events := make(chan Event, 16)
	l.events = events
}

func NewPostgresTransactionLogger(config PostgresDBParams) (TransactionLogger, error) {
	connStr := fmt.Sprintf("host=%s dbname=%s user=%s password=%s", config.host, config.dbName, config.user, config.password)

	db, err := sql.Open("postgres", connStr)

	if err != nil {
		return nil, fmt.Errorf("failed to open db: %w", err)
	}

	err = db.Ping()
	if err != nil {
		return nil, fmt.Errorf("failed to open db connection: %w", err)
	}

	logger := PostgresTransactionLogger{db: db}

	exists, err := logger.verifyTableExists()
	if err != nil {
		return nil, fmt.Errorf("failed to verify table exists: %w", err)
	}

	if !exists {
		if err = logger.createTable(); err != nil {
			return nil, fmt.Errorf("failed to create table: %w", err)
		}
	}

	return logger, nil

}
