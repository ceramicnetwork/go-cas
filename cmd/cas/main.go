package main

import (
	"context"
	"log"
	"math"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"
	"time"

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

	// Queue publishers
	var visibilityTimeout *time.Duration = nil
	if configVisibilityTimeout, found := os.LookupEnv("QUEUE_VISIBILITY_TIMEOUT"); found {
		if parsedVisibilityTimeout, err := time.ParseDuration(configVisibilityTimeout); err == nil {
			visibilityTimeout = &parsedVisibilityTimeout
		}
	}
	// Create the DLQ and prepare the redrive policy for the other queues
	deadLetterQueue, err := queue.NewPublisher(
		serverCtx,
		sqsClient,
		queue.PublisherOpts{QueueType: queue.QueueType_DLQ, VisibilityTimeout: visibilityTimeout},
	)
	if err != nil {
		log.Fatalf("main: failed to create dead-letter queue: %v", err)
	}
	dlqArn, err := queue.GetQueueArn(serverCtx, deadLetterQueue.QueueUrl, sqsClient)
	if err != nil {
		log.Fatalf("main: failed to fetch dead-letter queue arn: %v", err)
	}
	redrivePolicy := &queue.QueueRedrivePolicy{
		DeadLetterTargetArn: dlqArn,
		MaxReceiveCount:     queue.DefaultMaxReceiveCount,
	}
	// Create the remaining queues
	validateQueue, err := queue.NewPublisher(
		serverCtx,
		sqsClient,
		queue.PublisherOpts{
			QueueType:         queue.QueueType_Validate,
			VisibilityTimeout: visibilityTimeout,
			RedrivePolicy:     redrivePolicy,
		},
	)
	if err != nil {
		log.Fatalf("main: failed to create validate queue: %v", err)
	}
	readyQueue, err := queue.NewPublisher(
		serverCtx,
		sqsClient,
		queue.PublisherOpts{
			QueueType:         queue.QueueType_Ready,
			VisibilityTimeout: visibilityTimeout,
			RedrivePolicy:     redrivePolicy,
		},
	)
	if err != nil {
		log.Fatalf("main: failed to create ready queue: %v", err)
	}
	// The Batch queue will generally have a larger visibility timeout given the usually long batch linger times. It
	// will also allow a smaller maximum receive count before messages fall through to the DLQ. Detecting failures is
	// harder given the longer visibility timeout, so it's important that they be detected as soon as possible.
	batchVisibilityTimeout := visibilityTimeout
	if configVisibilityTimeout, found := os.LookupEnv("BATCH_QUEUE_VISIBILITY_TIMEOUT"); found {
		if parsedVisibilityTimeout, err := time.ParseDuration(configVisibilityTimeout); err == nil {
			batchVisibilityTimeout = &parsedVisibilityTimeout
		}
	}
	batchMaxReceiveCount := redrivePolicy.MaxReceiveCount
	if configMaxReceiveCount, found := os.LookupEnv("BATCH_QUEUE_MAX_RECEIVE_COUNT"); found {
		if parsedMaxReceiveCount, err := strconv.Atoi(configMaxReceiveCount); err == nil {
			batchMaxReceiveCount = parsedMaxReceiveCount
		}
	}
	batchQueue, err := queue.NewPublisher(
		serverCtx,
		sqsClient,
		queue.PublisherOpts{
			QueueType:         queue.QueueType_Batch,
			VisibilityTimeout: batchVisibilityTimeout,
			RedrivePolicy: &queue.QueueRedrivePolicy{
				DeadLetterTargetArn: dlqArn,
				MaxReceiveCount:     batchMaxReceiveCount,
			},
		},
	)
	if err != nil {
		log.Fatalf("main: failed to create batch queue: %v", err)
	}
	statusQueue, err := queue.NewPublisher(
		serverCtx,
		sqsClient,
		queue.PublisherOpts{
			QueueType:         queue.QueueType_Status,
			VisibilityTimeout: visibilityTimeout,
			RedrivePolicy:     redrivePolicy,
		},
	)
	if err != nil {
		log.Fatalf("main: failed to create status queue: %v", err)
	}
	// TODO: Could this become recursive since the failure handler also consumes from the DLQ? The inability to handle
	// failures could put messages back in the DLQ that are then re-consumed by the handler.
	failureQueue, err := queue.NewPublisher(
		serverCtx,
		sqsClient,
		queue.PublisherOpts{
			QueueType:         queue.QueueType_Failure,
			VisibilityTimeout: visibilityTimeout,
			RedrivePolicy:     redrivePolicy,
		},
	)
	if err != nil {
		log.Fatalf("main: failed to create failure queue: %v", err)
	}

	// Create the queue consumers. These consumers will be responsible for scaling event processing up based on load,
	// and also maintaining backpressure on the queues.

	// The Failure handling Service reads from the Failure and Dead-Letter queues
	failureHandlingService := services.NewFailureHandlingService(discordHandler)
	dlqConsumer := queue.NewConsumer(deadLetterQueue, failureHandlingService.DLQ, nil)
	failureConsumer := queue.NewConsumer(failureQueue, failureHandlingService.Failure, nil)

	// The Status Service reads from the Status queue and updates the Anchor DB
	statusService := services.NewStatusService(anchorDb)
	statusConsumer := queue.NewConsumer(statusQueue, statusService.Status, nil)

	// The Batching Service reads from the Ready queue and posts to the Batch queue
	anchorBatchSize := models.DefaultAnchorBatchSize
	if configAnchorBatchSize, found := os.LookupEnv("ANCHOR_BATCH_SIZE"); found {
		if parsedAnchorBatchSize, err := strconv.Atoi(configAnchorBatchSize); err == nil {
			anchorBatchSize = parsedAnchorBatchSize
		}
	}
	// Allow two batches worth of requests to be read and processed in parallel. This prevents a small number of workers
	// from waiting on an incomplete batch to fill up without any more workers available to add to the batch.
	maxBatchQueueWorkers := anchorBatchSize * 2
	maxReceivedMessages := math.Ceil(float64(maxBatchQueueWorkers) * 1.2)
	maxInflightRequests := math.Ceil(maxReceivedMessages / 10)

	batchingService := services.NewBatchingService(serverCtx, batchQueue, metricService)
	batchingConsumer := queue.NewConsumer(readyQueue, batchingService.Batch, &queue.ConsumerOpts{
		MaxReceivedMessages: int(maxReceivedMessages),
		MaxWorkers:          maxBatchQueueWorkers,
		MaxInflightRequests: int(maxInflightRequests),
	})

	// The Validation Service reads from the Validate queue and posts to the Ready and Status queues
	validationService := services.NewValidationService(stateDb, readyQueue, statusQueue, metricService)
	validationConsumer := queue.NewConsumer(validateQueue, validationService.Validate, nil)

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
