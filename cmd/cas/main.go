package main

import (
	"context"
	"log"
	"os"
	"sync"

	"github.com/joho/godotenv"

	"github.com/aws/aws-sdk-go-v2/service/sqs"

	"github.com/ceramicnetwork/go-cas/common/db"
	"github.com/ceramicnetwork/go-cas/common/queue"
	"github.com/ceramicnetwork/go-cas/common/utils"
	"github.com/ceramicnetwork/go-cas/models"
	"github.com/ceramicnetwork/go-cas/services"
)

func main() {
	if err := godotenv.Load("env/.env"); err != nil {
		log.Fatal("Error loading .env file", err)
	}
	log.SetFlags(log.Ldate | log.Ltime | log.Lshortfile)

	awsCfg, err := utils.AwsConfig()
	if err != nil {
		log.Fatalf("newCeramicLoader: error creating aws cfg: %v", err)
	}

	anchorDb := db.NewAnchorDb(db.AnchorDbOpts{
		Host:     os.Getenv("PG_HOST"),
		Port:     os.Getenv("PG_PORT"),
		User:     os.Getenv("PG_USER"),
		Password: os.Getenv("PG_PASSWORD"),
		Name:     os.Getenv("PG_DB"),
	})

	// Use override endpoint, if specified, for state DB so that we can store jobs locally, while hitting regular AWS
	// endpoints for other operations. This allows local testing without affecting live processes in AWS.
	dbAwsCfg := awsCfg
	stateDbEndpoint := os.Getenv("DB_AWS_ENDPOINT")
	if len(stateDbEndpoint) > 0 {
		log.Printf("using custom state db endpoint: %s", stateDbEndpoint)
		dbAwsCfg, err = utils.AwsConfigWithOverride(stateDbEndpoint)
		if err != nil {
			log.Fatalf("failed to create aws cfg: %v", err)
		}
	}
	stateDb := db.NewStateDb(dbAwsCfg)

	// Flow:
	// ====
	// 1. Request polling service:
	//	- Poll anchor DB for new requests
	//  - Post requests to Validate queue
	// 2. Validation service:
	//  - Deduplicate request stream/CIDs
	//  - Post requests to Ready queue
	// 3. Batching service:
	//	- Read requests from Ready queue
	//  - Accumulate requests until either batch is full or time runs out
	//  - Post batches to Batch queue

	// HTTP clients
	sqsClient := sqs.NewFromConfig(awsCfg)

	// Queue publishers
	validateQueue, err := queue.NewPublisher(models.QueueType_Validate, sqsClient)
	if err != nil {
		log.Fatalf("failed to create validate queue: %v", err)
	}
	readyQueue, err := queue.NewPublisher(models.QueueType_Ready, sqsClient)
	if err != nil {
		log.Fatalf("failed to create ready queue: %v", err)
	}
	batchQueue, err := queue.NewPublisher(models.QueueType_Batch, sqsClient)
	if err != nil {
		log.Fatalf("failed to create batch queue: %v", err)
	}

	// Start the queue consumers. These consumers will be responsible for scaling event processing up based on load, and
	// also maintaining backpressure on the queues.

	// The Batching Service reads from the Ready queue and posts to the Batch queue
	batchingService := services.NewBatchingService(batchQueue)
	queue.NewConsumer(readyQueue, batchingService.Batch).Start()

	// The Validation Service reads from the Validate queue and posts to the Ready queue
	validationService := services.NewValidationService(stateDb, readyQueue)
	queue.NewConsumer(validateQueue, validationService.Validate).Start()

	// Start the polling services last
	wg := sync.WaitGroup{}
	wg.Add(1)
	// Poll from Anchor DB and post to the Validate queue to kick-off processing
	go services.NewRequestPoller(anchorDb, stateDb, validateQueue).Run(context.Background())
	wg.Wait()
}
