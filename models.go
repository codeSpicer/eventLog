package main

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"
)

// Event represents a single event in the system
type Event struct {
	Timestamp time.Time       `json:"timestamp"`
	UserID    int64           `json:"user_id"`
	EventType string          `json:"event_type"`
	Payload   json.RawMessage `json:"payload"`
}

// QueryFilters represents filters for querying events
type QueryFilters struct {
	EventType string
	From      time.Time
	To        time.Time
}

// String returns the event in the required output format
func (e *Event) String() string {
	return fmt.Sprintf("%s | %d | %s | %s",
		e.Timestamp.Format(time.RFC3339),
		e.UserID,
		e.EventType,
		string(e.Payload))
}

// ParseEvent parses a line from the input file into an Event
func ParseEvent(line string) (*Event, error) {
	// Split by " | " delimiter
	parts := strings.Split(line, " | ")
	if len(parts) != 4 {
		return nil, fmt.Errorf("invalid format: expected 4 parts, got %d", len(parts))
	}
	
	// Parse timestamp
	timestamp, err := time.Parse(time.RFC3339, strings.TrimSpace(parts[0]))
	if err != nil {
		return nil, fmt.Errorf("invalid timestamp: %v", err)
	}
	
	// Parse user ID
	userID, err := strconv.ParseInt(strings.TrimSpace(parts[1]), 10, 64)
	if err != nil {
		return nil, fmt.Errorf("invalid user ID: %v", err)
	}
	
	// Event type
	eventType := strings.TrimSpace(parts[2])
	if eventType == "" {
		return nil, fmt.Errorf("empty event type")
	}
	
	// Parse payload JSON
	payloadStr := strings.TrimSpace(parts[3])
	var payload json.RawMessage
	if err := json.Unmarshal([]byte(payloadStr), &payload); err != nil {
		return nil, fmt.Errorf("invalid JSON payload: %v", err)
	}
	
	return &Event{
		Timestamp: timestamp,
		UserID:    userID,
		EventType: eventType,
		Payload:   payload,
	}, nil
}

// IsEmpty checks if QueryFilters has any active filters
func (qf *QueryFilters) IsEmpty() bool {
	return qf.EventType == "" && qf.From.IsZero() && qf.To.IsZero()
}

// Validate checks if the query filters are valid
func (qf *QueryFilters) Validate() error {
	if !qf.From.IsZero() && !qf.To.IsZero() && qf.From.After(qf.To) {
		return fmt.Errorf("from time cannot be after to time")
	}
	return nil
}