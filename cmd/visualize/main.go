package main

import (
	"fmt"
	"math/rand"
	"strings"

	"github.com/ski-akash/dist-kv-store/internal/hashing"
)

func main() {
	// 1. Initialize the ring with 150 virtual nodes per server
	ring := hashing.NewHashRing(150)
	servers := []string{"Server-A", "Server-B", "Server-C"}

	for _, s := range servers {
		ring.AddNode(s)
	}

	fmt.Println("--- INITIAL CLUSTER STATE (3 SERVERS) ---")

	// 2. Generate 100,000 random keys to test the distribution spread
	keyCount := 100000
	initialDistribution := make(map[string]int)

	// We will track a few specific keys to see what happens to them later
	sampleKeys := []string{"user:101", "movie:inception", "cart:456"}
	sampleTracking := make(map[string]string)

	for i := 0; i < keyCount; i++ {
		key := fmt.Sprintf("key:%d", rand.Int())
		node := ring.GetNode(key)
		initialDistribution[node]++
	}

	// Track our specific samples
	for _, key := range sampleKeys {
		sampleTracking[key] = ring.GetNode(key)
		fmt.Printf("Sample %-16s -> routed to %s\n", "'"+key+"'", sampleTracking[key])
	}

	fmt.Println("\nKey Distribution (100,000 total keys):")
	printChart(initialDistribution, keyCount)

	// 3. Simulate a Node Crash
	fmt.Println("\n--- SIMULATING CRASH: Removing Server-B ---")
	ring.RemoveNode("Server-B")

	// 4. Re-evaluate the exact same 100,000 keys
	// We reset the random seed so we generate the exact same keys as before
	rand.Seed(1)
	newDistribution := make(map[string]int)
	keysMoved := 0

	for i := 0; i < keyCount; i++ {
		key := fmt.Sprintf("key:%d", rand.Int())
		newNode := ring.GetNode(key)
		newDistribution[newNode]++
	}

	// Calculate how many keys migrated
	keysMoved = initialDistribution["Server-B"]

	fmt.Println("\nNew Key Distribution (Server-B keys redistributed):")
	printChart(newDistribution, keyCount)

	fmt.Printf("\nTotal Keys Migrated: %d (%.2f%% of total)\n", keysMoved, float64(keysMoved)/float64(keyCount)*100)

	// Check our specific samples again
	fmt.Println("\nSample Key Status Post-Crash:")
	for _, key := range sampleKeys {
		newNode := ring.GetNode(key)
		oldNode := sampleTracking[key]
		status := "STAYED"
		if newNode != oldNode {
			status = "MOVED "
		}
		fmt.Printf("Sample %-16s -> originally on %-8s | now on %-8s [%s]\n", "'"+key+"'", oldNode, newNode, status)
	}
}

// printChart is a helper to draw a nice terminal bar chart
func printChart(dist map[string]int, total int) {
	for node, count := range dist {
		percentage := float64(count) / float64(total) * 100
		bar := strings.Repeat("█", int(percentage)) // 1 block per 1%
		fmt.Printf("%-10s: %5d keys (%5.2f%%) | %s\n", node, count, percentage, bar)
	}
}
