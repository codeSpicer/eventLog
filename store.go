package main

import (
	"bufio"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

// EventStore manages event storage and retrieval
type EventStore struct {
	db         *sql.DB
	insertStmt *sql.Stmt
}

// NewEventStore creates a new EventStore with SQLite backend
func NewEventStore(dbPath string) (*EventStore, error) {
	// Open SQLite database
	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open database: %v", err)
	}

	// Configure SQLite for performance
	pragmas := []string{
		"PRAGMA journal_mode = WAL",    // Write-ahead logging for better concurrency
		"PRAGMA synchronous = NORMAL",  // Balance safety and performance
		"PRAGMA cache_size = 10000",    // 10MB cache
		"PRAGMA temp_store = MEMORY",   // Use memory for temporary tables
		"PRAGMA mmap_size = 268435456", // 256MB memory-mapped I/O
	}

	for _, pragma := range pragmas {
		if _, err := db.Exec(pragma); err != nil {
			db.Close()
			return nil, fmt.Errorf("failed to set pragma: %v", err)
		}
	}

	// Create table if not exists
	createTableSQL := `
	CREATE TABLE IF NOT EXISTS events (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		user_id INTEGER NOT NULL,
		timestamp TEXT NOT NULL,
		event_type TEXT NOT NULL,
		payload TEXT NOT NULL
	);`

	if _, err := db.Exec(createTableSQL); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to create table: %v", err)
	}

	// Create indexes for fast queries
	indexes := []string{
		"CREATE INDEX IF NOT EXISTS idx_user_timestamp ON events(user_id, timestamp)",
		"CREATE INDEX IF NOT EXISTS idx_user_type_timestamp ON events(user_id, event_type, timestamp)",
	}

	for _, indexSQL := range indexes {
		if _, err := db.Exec(indexSQL); err != nil {
			db.Close()
			return nil, fmt.Errorf("failed to create index: %v", err)
		}
	}

	// Prepare insert statement
	insertStmt, err := db.Prepare(`
		INSERT INTO events (user_id, timestamp, event_type, payload) 
		VALUES (?, ?, ?, ?)
	`)
	if err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to prepare insert statement: %v", err)
	}

	return &EventStore{
		db:         db,
		insertStmt: insertStmt,
	}, nil
}

// Close closes the database connection
func (es *EventStore) Close() error {
	if es.insertStmt != nil {
		es.insertStmt.Close()
	}
	if es.db != nil {
		return es.db.Close()
	}
	return nil
}

// Record ingests events from a file into the database
func (es *EventStore) Record(filename string) (int, error) {
	file, err := os.Open(filename)
	if err != nil {
		return 0, fmt.Errorf("failed to open file: %v", err)
	}
	defer file.Close()

	// Begin transaction for batch insert
	tx, err := es.db.Begin()
	if err != nil {
		return 0, fmt.Errorf("failed to begin transaction: %v", err)
	}
	defer tx.Rollback()

	// Use transaction version of prepared statement
	stmt := tx.Stmt(es.insertStmt)
	defer stmt.Close()

	scanner := bufio.NewScanner(file)
	count := 0
	batchSize := 0
	const maxBatchSize = 10000

	for scanner.Scan() {
		line := scanner.Text()
		if line == "" {
			continue // Skip empty lines
		}

		event, err := ParseEvent(line)
		if err != nil {
			fmt.Printf("Warning: Skipping invalid line: %v\n", err)
			continue
		}

		_, err = stmt.Exec(
			event.UserID,
			event.Timestamp.Format(time.RFC3339),
			event.EventType,
			string(event.Payload),
		)
		if err != nil {
			return count, fmt.Errorf("failed to insert event: %v", err)
		}

		count++
		batchSize++

		// Commit in batches to manage memory and provide progress
		if batchSize >= maxBatchSize {
			if err = tx.Commit(); err != nil {
				return count, fmt.Errorf("failed to commit batch: %v", err)
			}

			fmt.Printf("Processed %d events...\n", count)

			// Start new transaction
			tx, err = es.db.Begin()
			if err != nil {
				return count, fmt.Errorf("failed to begin new transaction: %v", err)
			}
			stmt = tx.Stmt(es.insertStmt)
			batchSize = 0
		}
	}

	if err := scanner.Err(); err != nil {
		return count, fmt.Errorf("error reading file: %v", err)
	}

	// Commit remaining events
	if err = tx.Commit(); err != nil {
		return count, fmt.Errorf("failed to commit final batch: %v", err)
	}

	return count, nil
}

// Query retrieves events for a specific user with optional filters
func (es *EventStore) Query(userID int64, filters QueryFilters) (int, error) {
	if err := filters.Validate(); err != nil {
		return 0, fmt.Errorf("invalid filters: %v", err)
	}

	// Build dynamic query based on filters
	query := `
		SELECT timestamp, user_id, event_type, payload 
		FROM events 
		WHERE user_id = ?`

	args := []interface{}{userID}

	if filters.EventType != "" {
		query += " AND event_type = ?"
		args = append(args, filters.EventType)
	}

	if !filters.From.IsZero() {
		query += " AND timestamp >= ?"
		args = append(args, filters.From.Format(time.RFC3339))
	}

	if !filters.To.IsZero() {
		query += " AND timestamp <= ?"
		args = append(args, filters.To.Format(time.RFC3339))
	}

	query += " ORDER BY timestamp"

	// Execute query
	rows, err := es.db.Query(query, args...)
	if err != nil {
		return 0, fmt.Errorf("query failed: %v", err)
	}
	defer rows.Close()

	count := 0
	for rows.Next() {
		var timestampStr string
		var payloadStr string
		var event Event

		err := rows.Scan(&timestampStr, &event.UserID, &event.EventType, &payloadStr)
		if err != nil {
			return count, fmt.Errorf("failed to scan row: %v", err)
		}

		// Parse timestamp
		event.Timestamp, err = time.Parse(time.RFC3339, timestampStr)
		if err != nil {
			return count, fmt.Errorf("failed to parse timestamp: %v", err)
		}

		event.Payload = json.RawMessage(payloadStr)

		// Output in required format
		fmt.Println(event.String())
		count++
	}

	if err = rows.Err(); err != nil {
		return count, fmt.Errorf("rows iteration error: %v", err)
	}

	return count, nil
}

// GetStats returns basic statistics about the stored events
func (es *EventStore) GetStats() (map[string]interface{}, error) {
	stats := make(map[string]interface{})

	// Total events
	var totalEvents int
	err := es.db.QueryRow("SELECT COUNT(*) FROM events").Scan(&totalEvents)
	if err != nil {
		return nil, err
	}
	stats["total_events"] = totalEvents

	// Unique users
	var uniqueUsers int
	err = es.db.QueryRow("SELECT COUNT(DISTINCT user_id) FROM events").Scan(&uniqueUsers)
	if err != nil {
		return nil, err
	}
	stats["unique_users"] = uniqueUsers

	// Date range
	var minTime, maxTime string
	err = es.db.QueryRow("SELECT MIN(timestamp), MAX(timestamp) FROM events").Scan(&minTime, &maxTime)
	if err != nil && err != sql.ErrNoRows {
		return nil, err
	}
	stats["time_range"] = map[string]string{"from": minTime, "to": maxTime}

	return stats, nil
}
