package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/joho/godotenv"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/sqs"

	"github.com/ceramicnetwork/go-cas/common/aws/config"
	"github.com/ceramicnetwork/go-cas/common/aws/ddb"
	"github.com/ceramicnetwork/go-cas/common/aws/queue"
	"github.com/ceramicnetwork/go-cas/common/db"
	"github.com/ceramicnetwork/go-cas/common/metrics"
	"github.com/ceramicnetwork/go-cas/common/notifs"
	"github.com/ceramicnetwork/go-cas/models"
	"github.com/ceramicnetwork/go-cas/services"
)

func main() {
	envFile := "env/.env"
	if envTag, found := os.LookupEnv(models.Env_EnvTag); found {
		envFile += "." + envTag
	}
	if err := godotenv.Load(envFile); err != nil {
		log.Fatalf("Error loading %s: %v", envFile, err)
	}
	log.SetFlags(log.Ldate | log.Ltime | log.Lshortfile)

	// Set up a server context
	serverCtx, serverCtxCancel := context.WithCancel(context.Background())

	awsCfg, err := config.AwsConfig(serverCtx)
	if err != nil {
		log.Fatalf("main: error creating aws cfg: %v", err)
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
		log.Printf("main: using custom state db endpoint: %s", stateDbEndpoint)
		dbAwsCfg, err = config.AwsConfigWithOverride(serverCtx, stateDbEndpoint)
		if err != nil {
			log.Fatalf("main: failed to create aws cfg: %v", err)
		}
	}

	// HTTP clients
	dynamoDbClient := dynamodb.NewFromConfig(dbAwsCfg)
	sqsClient := sqs.NewFromConfig(awsCfg)

	stateDb := ddb.NewStateDb(serverCtx, dynamoDbClient)
	jobDb := ddb.NewJobDb(serverCtx, dynamoDbClient)

	discordHandler, err := notifs.NewDiscordHandler()
	if err != nil {
		log.Fatalf("main: failed to create discord handler: %v", err)
	}

	collectorHost := os.Getenv("COLLECTOR_HOST")
	metricService, err := metrics.NewMetricService(serverCtx, collectorHost)
	if err != nil {
		log.Fatalf("main: failed to create metric service: %v", err)
	}

	// Flow:
	// ====
	// 1. Validation service:
	//	- Read requests from Validate queue
	//  - Deduplicate request stream/CIDs
	//  - Post requests to Ready queue
	// 2. Batching service:
	//	- Read requests from Ready queue
	//  - Accumulate requests until either batch is full or time runs out
	//  - Post batches to Batch queue
	// 3. Failure handling service:
	//  - Monitor the DLQ
	//  - Raise Discord alert for messages dropping through to the DLQ

	// Queue publishers

	// Create the DLQ and prepare the redrive policy for the other queues
	deadLetterQueue, err := queue.NewPublisher(serverCtx, queue.QueueType_DLQ, sqsClient, nil)
	if err != nil {
		log.Fatalf("main: failed to create dead-letter queue: %v", err)
	}
	dlqArn, err := queue.GetQueueArn(serverCtx, deadLetterQueue.QueueUrl, sqsClient)
	if err != nil {
		log.Fatalf("main: failed to fetch dead-letter queue arn: %v", err)
	}
	redrivePolicy := &queue.QueueRedrivePolicy{
		DeadLetterTargetArn: dlqArn,
		MaxReceiveCount:     queue.QueueMaxReceiveCount,
	}
	validateQueue, err := queue.NewPublisher(serverCtx, queue.QueueType_Validate, sqsClient, redrivePolicy)
	if err != nil {
		log.Fatalf("main: failed to create validate queue: %v", err)
	}
	readyQueue, err := queue.NewPublisher(serverCtx, queue.QueueType_Ready, sqsClient, redrivePolicy)
	if err != nil {
		log.Fatalf("main: failed to create ready queue: %v", err)
	}
	batchQueue, err := queue.NewPublisher(serverCtx, queue.QueueType_Batch, sqsClient, redrivePolicy)
	if err != nil {
		log.Fatalf("main: failed to create batch queue: %v", err)
	}
	statusQueue, err := queue.NewPublisher(serverCtx, queue.QueueType_Status, sqsClient, redrivePolicy)
	if err != nil {
		log.Fatalf("main: failed to create status queue: %v", err)
	}
	// TODO: Could this become recursive since the failure handler also consumes from the DLQ? The inability to handle
	// failures could put messages back in the DLQ that are then re-consumed by the handler.
	failureQueue, err := queue.NewPublisher(serverCtx, queue.QueueType_Failure, sqsClient, redrivePolicy)
	if err != nil {
		log.Fatalf("main: failed to create failure queue: %v", err)
	}

	// Create the queue consumers. These consumers will be responsible for scaling event processing up based on load,
	// and also maintaining backpressure on the queues.

	// The Failure handling Service reads from the Failure and Dead-Letter queues
	failureHandlingService := services.NewFailureHandlingService(discordHandler)
	dlqConsumer := queue.NewConsumer(deadLetterQueue, failureHandlingService.DLQ)
	failureConsumer := queue.NewConsumer(failureQueue, failureHandlingService.Failure)

	// The Status Service reads from the Status queue and updates the Anchor DB
	statusService := services.NewStatusService(anchorDb)
	statusConsumer := queue.NewConsumer(statusQueue, statusService.Status)

	// The Batching Service reads from the Ready queue and posts to the Batch queue
	batchingService := services.NewBatchingService(serverCtx, batchQueue, metricService)
	batchingConsumer := queue.NewConsumer(readyQueue, batchingService.Batch)

	// The Validation Service reads from the Validate queue and posts to the Ready and Status queues
	validationService := services.NewValidationService(stateDb, readyQueue, statusQueue, metricService)
	validationConsumer := queue.NewConsumer(validateQueue, validationService.Validate)

	wg := sync.WaitGroup{}
	wg.Add(2)

	// Set up graceful shutdown
	go func() {
		defer wg.Done()

		interruptCh := make(chan os.Signal, 1)
		signal.Notify(interruptCh, syscall.SIGTERM)
		<-interruptCh
		log.Println("main: shutdown started")

		// Shut down services in the order in which data goes through the pipeline:
		//  - validation
		//  - batching
		//  - status updates
		//  - failure handling
		//  - DLQ
		validationConsumer.Shutdown()
		batchingConsumer.Shutdown()
		statusConsumer.Shutdown()
		failureConsumer.Shutdown()
		dlqConsumer.Shutdown()

		// Flush metrics
		metricService.Shutdown(serverCtx)

		// Cancel the server context
		serverCtxCancel()

		log.Println("main: shutdown complete")
	}()

	// Start pipeline components in the opposite order in which data goes through
	go func() {
		defer wg.Done()
		// Monitor the Batch queue and spawn anchor workers accordingly
		services.NewWorkerService(queue.NewMonitor(batchQueue.QueueUrl, sqsClient), jobDb).Run(serverCtx)
	}()

	dlqConsumer.Start()
	failureConsumer.Start()
	statusConsumer.Start()
	batchingConsumer.Start()
	validationConsumer.Start()

	wg.Wait()
}
