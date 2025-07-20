package main

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"time"
)

type EventPayload struct {
	IP       string  `json:"ip,omitempty"`
	Item     string  `json:"item,omitempty"`
	Price    float64 `json:"price,omitempty"`
	Location string  `json:"location,omitempty"`
	Device   string  `json:"device,omitempty"`
	Status   string  `json:"status,omitempty"`
	Duration int     `json:"duration,omitempty"`
	Page     string  `json:"page,omitempty"`
}

func generateTestData(filename string, numEvents int) error {
	file, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer file.Close()

	rand.Seed(time.Now().UnixNano())

	// Event types with different probabilities
	eventTypes := []string{"login", "purchase", "logout", "page_view", "search", "download", "signup", "error"}
	eventWeights := []int{15, 10, 12, 30, 20, 5, 3, 5} // page_view is most common
	totalWeight := 0
	for _, w := range eventWeights {
		totalWeight += w
	}

	// Sample data pools
	ips := []string{"192.168.1.1", "10.0.0.1", "172.16.0.1", "203.0.113.1", "198.51.100.1"}
	items := []string{"A123", "B456", "C789", "D012", "E345", "F678", "G901", "H234"}
	locations := []string{"US", "UK", "CA", "AU", "DE", "FR", "JP", "IN"}
	devices := []string{"mobile", "desktop", "tablet", "smart-tv"}
	pages := []string{"/home", "/products", "/cart", "/checkout", "/profile", "/search", "/help"}

	baseTime := time.Date(2023, 8, 14, 10, 0, 0, 0, time.UTC)

	// Generate events with realistic distribution
	userCount := 10000 // 10k unique users

	for i := 0; i < numEvents; i++ {
		// Generate realistic timestamp (events spread over 24 hours)
		offsetMinutes := rand.Intn(24 * 60) // 24 hours in minutes
		timestamp := baseTime.Add(time.Duration(offsetMinutes) * time.Minute)

		// User distribution: 80% of events from 20% of users (Pareto principle)
		var userID int
		if rand.Float64() < 0.8 {
			// Heavy users (20% of user base)
			userID = rand.Intn(userCount / 5)
		} else {
			// Light users (80% of user base)
			userID = userCount/5 + rand.Intn(userCount*4/5)
		}

		// Select event type based on weights
		eventType := weightedChoice(eventTypes, eventWeights, totalWeight)

		// Generate payload based on event type
		var payload EventPayload

		switch eventType {
		case "login":
			payload = EventPayload{
				IP:     ips[rand.Intn(len(ips))],
				Device: devices[rand.Intn(len(devices))],
			}
		case "purchase":
			payload = EventPayload{
				Item:  items[rand.Intn(len(items))],
				Price: float64(rand.Intn(10000)) / 100.0, // $0.00 to $99.99
			}
		case "logout":
			payload = EventPayload{
				Duration: rand.Intn(3600), // 0 to 1 hour in seconds
			}
		case "page_view":
			payload = EventPayload{
				Page:   pages[rand.Intn(len(pages))],
				Device: devices[rand.Intn(len(devices))],
			}
		case "search":
			payload = EventPayload{
				Page: "/search",
			}
		case "download":
			payload = EventPayload{
				Item: items[rand.Intn(len(items))],
			}
		case "signup":
			payload = EventPayload{
				IP:       ips[rand.Intn(len(ips))],
				Location: locations[rand.Intn(len(locations))],
			}
		case "error":
			payload = EventPayload{
				Status: fmt.Sprintf("%d", 400+rand.Intn(200)), // 400-599 error codes
				Page:   pages[rand.Intn(len(pages))],
			}
		}

		payloadBytes, _ := json.Marshal(payload)

		// Write in the required format
		fmt.Fprintf(file, "%s | %d | %s | %s\n",
			timestamp.Format(time.RFC3339),
			userID,
			eventType,
			string(payloadBytes))

		// Progress indicator
		if i%100000 == 0 {
			fmt.Printf("Generated %d events...\n", i)
		}
	}

	fmt.Printf("Successfully generated %d events in %s\n", numEvents, filename)
	return nil
}

func weightedChoice(choices []string, weights []int, totalWeight int) string {

	r := rand.Intn(totalWeight)
	cumulative := 0

	for i, weight := range weights {
		cumulative += weight
		if r < cumulative {
			return choices[i]
		}
	}

	return choices[0] // fallback
}

func main() {
	if len(os.Args) < 2 {
		fmt.Println("Usage: go run generate_test_data.go <output_file> [num_events]")
		os.Exit(1)
	}

	filename := os.Args[1]
	numEvents := 1000000 // default 1M

	if len(os.Args) > 2 {
		fmt.Sscanf(os.Args[2], "%d", &numEvents)
	}

	fmt.Printf("Generating %d events...\n", numEvents)
	start := time.Now()

	err := generateTestData(filename, numEvents)
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		os.Exit(1)
	}

	duration := time.Since(start)
	fmt.Printf("Completed in %v\n", duration)

	// Calculate file size
	stat, _ := os.Stat(filename)
	fmt.Printf("File size: %.2f MB\n", float64(stat.Size())/(1024*1024))
}
