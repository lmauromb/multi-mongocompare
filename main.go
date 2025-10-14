package main

import (
	"bufio"
	"context"
	"fmt"
	"log"
	"os"
	"sync"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"golang.org/x/sync/semaphore"

	flag "github.com/spf13/pflag"
)

type CollectionInfo struct {
	DBName         string
	CollectionName string
	FullName       string // dbName.collName
}

type CountResult struct {
	CollectionName string
	Match          bool
	SourceCount    int64
	TargetCount    int64
}

func main() {
	var sourceMongoURI, targetMongoURI, outputFile string
	var nWorkers int

	flag.StringVar(&sourceMongoURI, "sourceMongoURI", "", "source connection string")
	flag.StringVar(&targetMongoURI, "targetMongoURI", "", "target connection string")
	flag.IntVar(&nWorkers, "nWorkers", 1, "number of workers to use")
	flag.StringVar(&outputFile, "outputFile", "results.csv", "output file")

	flag.Parse()

	// Connect to MongoDB
	sourceClient, err := connectToMongoDB(sourceMongoURI)
	if err != nil {
		log.Fatal("Failed to connect to MongoDB:", err)
	}
	defer sourceClient.Disconnect(context.Background())

	targetClient, err := connectToMongoDB(targetMongoURI)
	if err != nil {
		log.Fatal("Failed to connect to MongoDB:", err)
	}
	defer sourceClient.Disconnect(context.Background())

	// Gather all databases and collections
	collections, err := gatherCollections(sourceClient)
	if err != nil {
		log.Fatal("Failed to gather collections:", err)
	}

	if len(collections) == 0 {
		fmt.Println("No collections found")
		return
	}

	fmt.Printf("Found %d collections, processing with %d workers\n", len(collections), nWorkers)

	// Create output file
	file, err := os.Create(outputFile)
	if err != nil {
		log.Fatal("Failed to create output file:", err)
	}
	defer file.Close()

	writer := bufio.NewWriter(file)
	defer writer.Flush()

	// write header
	_, err = writer.WriteString("namespace, match, sourceCount, targetCount\n")
	if err != nil {
		log.Fatal("Failed to write header to file:", err)
	}

	// Process collections with workers using semaphore
	err = processCollectionsWithSemaphore(sourceClient, targetClient, collections, nWorkers, writer)
	if err != nil {
		log.Fatal("Failed to process collections:", err)
	}

	fmt.Printf("Results written to %s\n", outputFile)
}

// connectToMongoDB establishes connection to MongoDB
func connectToMongoDB(uri string) (*mongo.Client, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	clientOptions := options.Client().ApplyURI(uri)
	client, err := mongo.Connect(ctx, clientOptions)
	if err != nil {
		return nil, err
	}

	// Ping the database to verify connection
	err = client.Ping(ctx, nil)
	if err != nil {
		return nil, err
	}

	return client, nil
}

// gatherCollections retrieves all databases and collections
func gatherCollections(client *mongo.Client) ([]CollectionInfo, error) {
	ctx := context.Background()
	var collections []CollectionInfo

	// List all databases
	databases, err := client.ListDatabaseNames(ctx, map[string]interface{}{})
	if err != nil {
		return nil, err
	}

	// For each database, list all collections
	for _, dbName := range databases {
		// Skip system databases if desired
		if dbName == "admin" || dbName == "local" || dbName == "config" {
			continue
		}

		db := client.Database(dbName)
		collectionNames, err := db.ListCollectionNames(ctx, map[string]interface{}{})
		if err != nil {
			log.Printf("Warning: Failed to list collections for database %s: %v", dbName, err)
			continue
		}

		// Add each collection to our slice
		for _, collName := range collectionNames {
			collections = append(collections, CollectionInfo{
				DBName:         dbName,
				CollectionName: collName,
				FullName:       fmt.Sprintf("%s.%s", dbName, collName),
			})
		}
	}

	return collections, nil
}

// processCollectionsWithSemaphore processes collections using semaphore for concurrency control
func processCollectionsWithSemaphore(sourceClient *mongo.Client, targetClient *mongo.Client, collections []CollectionInfo, nWorkers int, writer *bufio.Writer) error {
	ctx := context.Background()

	// Create semaphore to limit concurrent workers
	sem := semaphore.NewWeighted(int64(nWorkers))

	// Channel to collect results
	resultCh := make(chan CountResult, len(collections))
	errorCh := make(chan error, len(collections))

	var wg sync.WaitGroup

	// Start all collection counting goroutines
	for _, collection := range collections {
		wg.Add(1)

		go func(collInfo CollectionInfo) {
			defer wg.Done()

			// Acquire semaphore (blocks if all workers are busy)
			if err := sem.Acquire(ctx, 1); err != nil {
				errorCh <- fmt.Errorf("failed to acquire semaphore: %v", err)
				return
			}
			defer sem.Release(1) // Release semaphore when done

			sourceCollection := sourceClient.Database(collInfo.DBName).Collection(collInfo.CollectionName)
			targetCollection := targetClient.Database(collInfo.DBName).Collection(collInfo.CollectionName)

			// Count documents
			// count, err := countDocuments(client, collInfo)
			checkCountsResult, sourceCount, targetCount := checkCounts(sourceCollection, targetCollection)
			if !checkCountsResult {
				errorCh <- fmt.Errorf("Document counts don't match in %s. Source: %d, Target: %d\n", collInfo.FullName, sourceCount, targetCount)

				// return
			}

			// Send result to channel
			resultCh <- CountResult{
				CollectionName: collInfo.FullName,
				Match:          checkCountsResult,
				SourceCount:    sourceCount,
				TargetCount:    targetCount,
			}
		}(collection)
	}

	// Close channels when all goroutines complete
	go func() {
		wg.Wait()
		close(resultCh)
		close(errorCh)
	}()

	// Collect and write results
	var writeError error
	var writeMutex sync.Mutex

	// Process results as they come in
	for result := range resultCh {
		writeMutex.Lock()
		_, err := writer.WriteString(fmt.Sprintf("%s,%t,%d,%d\n", result.CollectionName, result.Match, result.SourceCount, result.TargetCount))
		if err != nil {
			writeError = err
		}
		writeMutex.Unlock()
	}

	// Check for any errors
	for err := range errorCh {
		log.Printf("Error: %v", err)
	}

	return writeError
}

// countDocuments counts documents in a specific collection
func countDocuments(client *mongo.Client, collInfo CollectionInfo) (int64, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	collection := client.Database(collInfo.DBName).Collection(collInfo.CollectionName)
	count, err := collection.CountDocuments(ctx, map[string]interface{}{})
	if err != nil {
		return 0, err
	}

	return count, nil
}

func checkCounts(sourceCollection *mongo.Collection, targetCollection *mongo.Collection) (bool, int64, int64) {
	sourceCount, err := sourceCollection.CountDocuments(context.TODO(), bson.D{})
	if err != nil {
		log.Fatal(err)
	}
	targetCount, err := targetCollection.CountDocuments(context.TODO(), bson.D{})
	if err != nil {
		log.Fatal(err)
	}

	if sourceCount == targetCount {
		return true, sourceCount, targetCount
	} else {
		return false, sourceCount, targetCount
	}
}
