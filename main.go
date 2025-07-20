package main

import (
	"flag"
	"fmt"
	"os"
	"strconv"
	"time"
)

func main() {
	if len(os.Args) < 2 {
		printUsage()
		os.Exit(1)
	}

	command := os.Args[1]
	
	switch command {
	case "record":
		handleRecord(os.Args[2:])
	case "query":
		handleQuery(os.Args[2:])
	default:
		fmt.Printf("Unknown command: %s\n", command)
		printUsage()
		os.Exit(1)
	}
}

func handleRecord(args []string) {
	if len(args) < 1 {
		fmt.Println("Usage: eventlog record <file>")
		os.Exit(1)
	}
	
	filename := args[0]
	
	// Check if file exists
	if _, err := os.Stat(filename); os.IsNotExist(err) {
		fmt.Printf("Error: File %s does not exist\n", filename)
		os.Exit(1)
	}
	
	fmt.Printf("Recording events from %s...\n", filename)
	
	// Initialize store
	store, err := NewEventStore("events.db")
	if err != nil {
		fmt.Printf("Error initializing store: %v\n", err)
		os.Exit(1)
	}
	defer store.Close()
	
	// Record events
	start := time.Now()
	count, err := store.Record(filename)
	if err != nil {
		fmt.Printf("Error recording events: %v\n", err)
		os.Exit(1)
	}
	
	duration := time.Since(start)
	fmt.Printf("Successfully recorded %d events in %v\n", count, duration)
}

func handleQuery(args []string) {
	if len(args) < 1 {
		fmt.Println("Usage: eventlog query <user-id> [--type=<event-type>] [--from=<ISO8601>] [--to=<ISO8601>]")
		os.Exit(1)
	}
	
	userID, err := strconv.ParseInt(args[0], 10, 64)
	if err != nil {
		fmt.Printf("Error: Invalid user ID: %s\n", args[0])
		os.Exit(1)
	}
	
	// Parse flags
	flagSet := flag.NewFlagSet("query", flag.ExitOnError)
	eventType := flagSet.String("type", "", "Filter by event type")
	fromStr := flagSet.String("from", "", "Filter events from this time (ISO8601)")
	toStr := flagSet.String("to", "", "Filter events to this time (ISO8601)")
	
	flagSet.Parse(args[1:])
	
	filters := QueryFilters{
		EventType: *eventType,
	}
	
	// Parse time filters
	if *fromStr != "" {
		filters.From, err = time.Parse(time.RFC3339, *fromStr)
		if err != nil {
			fmt.Printf("Error: Invalid from time format: %s\n", *fromStr)
			os.Exit(1)
		}
	}
	
	if *toStr != "" {
		filters.To, err = time.Parse(time.RFC3339, *toStr)
		if err != nil {
			fmt.Printf("Error: Invalid to time format: %s\n", *toStr)
			os.Exit(1)
		}
	}
	
	// Initialize store
	store, err := NewEventStore("events.db")
	if err != nil {
		fmt.Printf("Error initializing store: %v\n", err)
		os.Exit(1)
	}
	defer store.Close()
	
	// Query events
	start := time.Now()
	count, err := store.Query(userID, filters)
	if err != nil {
		fmt.Printf("Error querying events: %v\n", err)
		os.Exit(1)
	}
	
	duration := time.Since(start)
	fmt.Fprintf(os.Stderr, "Query completed: %d events in %v\n", count, duration)
}

func printUsage() {
	fmt.Println("Usage:")
	fmt.Println("  eventlog record <file>")
	fmt.Println("  eventlog query <user-id> [--type=<event-type>] [--from=<ISO8601>] [--to=<ISO8601>]")
	fmt.Println()
	fmt.Println("Examples:")
	fmt.Println("  eventlog record events.txt")
	fmt.Println("  eventlog query 42")
	fmt.Println("  eventlog query 42 --type=login")
	fmt.Println("  eventlog query 42 --from=2023-08-14T12:00:00Z --to=2023-08-14T13:00:00Z")
}