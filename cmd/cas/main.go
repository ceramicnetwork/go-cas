package main

import (
	"context"
	"log"
	"os"
	"sync"

	"github.com/joho/godotenv"

	"github.com/aws/aws-sdk-go-v2/service/sqs"

	"github.com/ceramicnetwork/go-cas/common/aws"
	"github.com/ceramicnetwork/go-cas/common/aws/queue"
	"github.com/ceramicnetwork/go-cas/common/db"
	"github.com/ceramicnetwork/go-cas/common/notifs"
	"github.com/ceramicnetwork/go-cas/services"
)

func main() {
	if err := godotenv.Load("env/.env"); err != nil {
		log.Fatal("Error loading .env file", err)
	}
	log.SetFlags(log.Ldate | log.Ltime | log.Lshortfile)

	awsCfg, err := aws.AwsConfig()
	if err != nil {
		log.Fatalf("error creating aws cfg: %v", err)
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
		dbAwsCfg, err = aws.AwsConfigWithOverride(stateDbEndpoint)
		if err != nil {
			log.Fatalf("failed to create aws cfg: %v", err)
		}
	}
	stateDb := db.NewStateDb(dbAwsCfg)

	discordHandler, err := notifs.NewDiscordHandler()
	if err != nil {
		log.Fatalf("failed to create discord handler: %v", err)
	}

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
	// 4. Failure handling service:
	//  - Monitor the DLQ
	//  - Raise Discord alert for messages dropping through to the DLQ

	// HTTP clients
	sqsClient := sqs.NewFromConfig(awsCfg)

	// Queue publishers

	// Create the DLQ and prepare the redrive policy for the other queues
	deadLetterQueue, err := queue.NewPublisher(queue.QueueType_DLQ, sqsClient, nil)
	if err != nil {
		log.Fatalf("failed to create dead-letter queue: %v", err)
	}
	dlqArn, err := queue.GetQueueArn(deadLetterQueue.QueueUrl, sqsClient)
	if err != nil {
		log.Fatalf("failed to fetch dead-letter queue arn: %v", err)
	}
	redrivePolicy := &queue.QueueRedrivePolicy{
		DeadLetterTargetArn: dlqArn,
		MaxReceiveCount:     queue.QueueMaxReceiveCount,
	}
	validateQueue, err := queue.NewPublisher(queue.QueueType_Validate, sqsClient, redrivePolicy)
	if err != nil {
		log.Fatalf("failed to create validate queue: %v", err)
	}
	readyQueue, err := queue.NewPublisher(queue.QueueType_Ready, sqsClient, redrivePolicy)
	if err != nil {
		log.Fatalf("failed to create ready queue: %v", err)
	}
	batchQueue, err := queue.NewPublisher(queue.QueueType_Batch, sqsClient, redrivePolicy)
	if err != nil {
		log.Fatalf("failed to create batch queue: %v", err)
	}
	// TODO: Could this become recursive since the failure handler also consumes from the DLQ? The inability to handle
	// failures could put messages back in the DLQ that are then re-consumed by the handler.
	failureQueue, err := queue.NewPublisher(queue.QueueType_Failure, sqsClient, redrivePolicy)
	if err != nil {
		log.Fatalf("failed to create failure queue: %v", err)
	}
	// Start the queue consumers. These consumers will be responsible for scaling event processing up based on load, and
	// also maintaining backpressure on the queues.

	// The Batching Service reads from the Ready queue and posts to the Batch queue
	batchingService := services.NewBatchingService(batchQueue)
	queue.NewConsumer(readyQueue, batchingService.Batch).Start()

	// The Validation Service reads from the Validate queue and posts to the Ready queue
	validationService := services.NewValidationService(stateDb, readyQueue)
	queue.NewConsumer(validateQueue, validationService.Validate).Start()

	// The Failure handling Service reads from the Failure and Dead-Letter queues
	failureHandlingService := services.NewFailureHandlingService(discordHandler)
	queue.NewConsumer(failureQueue, failureHandlingService.Failure).Start()
	queue.NewConsumer(deadLetterQueue, failureHandlingService.DLQ).Start()

	// Start the polling services last
	wg := sync.WaitGroup{}
	wg.Add(1)
	// Poll from Anchor DB and post to the Validate queue to kick-off processing
	go services.NewRequestPoller(anchorDb, stateDb, validateQueue).Run(context.Background())
	wg.Wait()
}
