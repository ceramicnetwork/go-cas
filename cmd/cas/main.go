package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"
	"time"

	"github.com/joho/godotenv"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/sqs"

	"github.com/ceramicnetwork/go-cas"
	"github.com/ceramicnetwork/go-cas/common/aws/config"
	"github.com/ceramicnetwork/go-cas/common/aws/ddb"
	"github.com/ceramicnetwork/go-cas/common/aws/queue"
	"github.com/ceramicnetwork/go-cas/common/db"
	"github.com/ceramicnetwork/go-cas/common/loggers"
	"github.com/ceramicnetwork/go-cas/common/metrics"
	"github.com/ceramicnetwork/go-cas/common/notifs"
	"github.com/ceramicnetwork/go-cas/models"
	"github.com/ceramicnetwork/go-cas/services"
)

func main() {
	envFile := "env/.env"
	if envTag, found := os.LookupEnv(cas.Env_EnvTag); found {
		envFile += "." + envTag
	}
	if err := godotenv.Load(envFile); err != nil {
		log.Fatalf("Error loading %s: %v", envFile, err)
	}

	// Set up a server context
	serverCtx, serverCtxCancel := context.WithCancel(context.Background())

	// set up logger
	logger := loggers.NewLogger()
	defer logger.Sync()

	awsCfg, err := config.AwsConfig(serverCtx)
	if err != nil {
		logger.Fatalf("error creating aws cfg: %v", err)
	}

	anchorDb := db.NewAnchorDb(logger)

	// HTTP clients
	dynamoDbClient := dynamodb.NewFromConfig(awsCfg)
	sqsClient := sqs.NewFromConfig(awsCfg)

	stateDb := ddb.NewStateDb(serverCtx, logger, dynamoDbClient)
	jobDb := ddb.NewJobDb(serverCtx, logger, dynamoDbClient)

	discordHandler, err := notifs.NewDiscordHandler(logger)
	if err != nil {
		logger.Fatalf("error creating discord handler: %v", err)
	}

	metricService, err := metrics.NewMetricService(serverCtx, logger, models.MetricsCallerName)
	if err != nil {
		logger.Fatalf("error creating metric service: %v", err)
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
		logger.Fatalf("error creating dead-letter queue: %v", err)
	}
	dlqArn, err := queue.GetQueueArn(serverCtx, deadLetterQueue.GetUrl(), sqsClient)
	if err != nil {
		logger.Fatalf("error fetching dead-letter queue arn: %v", err)
	}
	redrivePolicy := &queue.QueueRedrivePolicy{
		DeadLetterTargetArn: dlqArn,
		MaxReceiveCount:     queue.DefaultMaxReceiveCount,
	}
	// Failure queue
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
		logger.Fatalf("error creating failure queue: %v", err)
	}
	// Validate queue
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
		logger.Fatalf("error creating validate queue: %v", err)
	}
	// The Ready and Batch queues will need larger visibility timeouts than the other queues. Requests pulled from the
	// Ready queue will remain in flight for the batch linger duration. Batches from the Batch queue will remain in
	// flight as long as it takes for them to get anchored.
	// These queues will thus allow a smaller maximum receive count before messages fall through to the DLQ. Detecting
	// failures is harder given the longer visibility timeouts, so it's important that they be detected as soon as
	// possible.
	longQueueVisibilityTimeout := visibilityTimeout
	if configVisibilityTimeout, found := os.LookupEnv("LONG_QUEUE_VISIBILITY_TIMEOUT"); found {
		if parsedVisibilityTimeout, err := time.ParseDuration(configVisibilityTimeout); err == nil {
			longQueueVisibilityTimeout = &parsedVisibilityTimeout
		}
	}
	longQueueMaxReceiveCount := redrivePolicy.MaxReceiveCount
	if configMaxReceiveCount, found := os.LookupEnv("LONG_QUEUE_MAX_RECEIVE_COUNT"); found {
		if parsedMaxReceiveCount, err := strconv.Atoi(configMaxReceiveCount); err == nil {
			longQueueMaxReceiveCount = parsedMaxReceiveCount
		}
	}
	longQueueRedrivePolicy := &queue.QueueRedrivePolicy{
		DeadLetterTargetArn: dlqArn,
		MaxReceiveCount:     longQueueMaxReceiveCount,
	}
	readyQueue, err := queue.NewPublisher(
		serverCtx,
		sqsClient,
		queue.PublisherOpts{
			QueueType:         queue.QueueType_Ready,
			VisibilityTimeout: longQueueVisibilityTimeout,
			RedrivePolicy:     longQueueRedrivePolicy,
		},
	)
	if err != nil {
		logger.Fatalf("error creating ready queue: %v", err)
	}
	batchQueue, err := queue.NewPublisher(
		serverCtx,
		sqsClient,
		queue.PublisherOpts{
			QueueType:         queue.QueueType_Batch,
			VisibilityTimeout: longQueueVisibilityTimeout,
			RedrivePolicy:     longQueueRedrivePolicy,
		},
	)
	if err != nil {
		logger.Fatalf("error creating batch queue: %v", err)
	}
	// Status queue
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
		logger.Fatalf("error creating status queue: %v", err)
	}

	// Create utilization gauges for all the queues
	if err = metricService.QueueGauge(serverCtx, deadLetterQueue.GetName(), queue.NewMonitor(deadLetterQueue.GetUrl(), sqsClient)); err != nil {
		logger.Fatalf("error creating gauge for dead-letter queue: %v", err)
	}
	if err = metricService.QueueGauge(serverCtx, failureQueue.GetName(), queue.NewMonitor(failureQueue.GetUrl(), sqsClient)); err != nil {
		logger.Fatalf("error creating gauge for failure queue: %v", err)
	}
	if err = metricService.QueueGauge(serverCtx, validateQueue.GetName(), queue.NewMonitor(validateQueue.GetUrl(), sqsClient)); err != nil {
		logger.Fatalf("error creating gauge for validate queue: %v", err)
	}
	if err = metricService.QueueGauge(serverCtx, readyQueue.GetName(), queue.NewMonitor(readyQueue.GetUrl(), sqsClient)); err != nil {
		logger.Fatalf("error creating gauge for ready queue: %v", err)
	}
	batchMonitor := queue.NewMonitor(batchQueue.GetUrl(), sqsClient)
	if err = metricService.QueueGauge(serverCtx, batchQueue.GetName(), batchMonitor); err != nil {
		logger.Fatalf("error creating gauge for batch queue: %v", err)
	}
	if err = metricService.QueueGauge(serverCtx, statusQueue.GetName(), queue.NewMonitor(statusQueue.GetUrl(), sqsClient)); err != nil {
		logger.Fatalf("error creating gauge for status queue: %v", err)
	}

	// Create the queue consumers. These consumers will be responsible for scaling event processing up based on load and
	// also maintaining backpressure on the queues.

	// The Failure handling service reads from the Failure and Dead-Letter queues
	failureHandlingService := services.NewFailureHandlingService(discordHandler, metricService)
	dlqConsumer := queue.NewConsumer(logger, deadLetterQueue, failureHandlingService.DLQ, nil)
	failureConsumer := queue.NewConsumer(logger, failureQueue, failureHandlingService.Failure, nil)

	// The Status service reads from the Status queue and updates the Anchor DB
	statusService := services.NewStatusService(anchorDb)
	statusConsumer := queue.NewConsumer(logger, statusQueue, statusService.Status, nil)

	// The Batching service reads from the Ready queue and posts to the Batch queue
	anchorBatchSize := models.DefaultAnchorBatchSize
	if configAnchorBatchSize, found := os.LookupEnv(models.Env_AnchorBatchSize); found {
		if parsedAnchorBatchSize, err := strconv.Atoi(configAnchorBatchSize); err == nil {
			anchorBatchSize = parsedAnchorBatchSize
		}
	}
	// Launch a number of workers greater than the batch size. This prevents a small number of workers from waiting on
	// an incomplete batch to fill up because there aren't any workers available to add to the batch even when messages
	// are available in the queue. The 2 multiplier is arbitrary but will allow two batches worth of requests to be read
	// and processed in parallel.
	maxBatchQueueWorkers := anchorBatchSize * 2
	batchingService := services.NewBatchingService(serverCtx, logger, batchQueue, metricService)
	batchingConsumer := queue.NewConsumer(logger, readyQueue, batchingService.Batch, &maxBatchQueueWorkers)

	// The Validation service reads from the Validate queue and posts to the Ready and Status queues
	validationService := services.NewValidationService(logger, stateDb, readyQueue, statusQueue, metricService)
	validationConsumer := queue.NewConsumer(logger, validateQueue, validationService.Validate, nil)

	wg := sync.WaitGroup{}
	wg.Add(3)

	// Set up graceful shutdown
	go func() {
		defer wg.Done()

		interruptCh := make(chan os.Signal, 1)
		signal.Notify(interruptCh, syscall.SIGTERM)
		<-interruptCh
		logger.Infoln("shutdown started")

		// Shut down services in the order in which data goes through the pipeline:
		//  - validation
		//  - batching
		//  - status updates
		//  - failure handling
		//  - DLQ
		validationConsumer.Shutdown()

		// The Batching service needs a special shutdown procedure:
		//  - Start shutting down the queue consumer, which will prevent any new receive requests from being initiated.
		//  - Wait for any in flight receive requests to complete, which will be signaled via `WaitForRxShutdown()`.
		//  - Wait a few seconds (as a precaution) for any in flight messages to be picked up by workers, then flush the
		//    batch. This will cause any lingering workers to complete their processing and allow the consumer to fully
		//    shut down.
		batchWg := sync.WaitGroup{}
		batchWg.Add(2)
		go func() {
			defer batchWg.Done()
			batchingConsumer.Shutdown()
		}()
		go func() {
			defer batchWg.Done()
			batchingConsumer.WaitForRxShutdown()
			time.Sleep(5 * time.Second)
			batchingService.Flush()
		}()
		batchWg.Wait()

		statusConsumer.Shutdown()
		failureConsumer.Shutdown()
		dlqConsumer.Shutdown()

		// Flush metrics
		metricService.Shutdown(serverCtx)

		// Cancel the server context
		serverCtxCancel()

		logger.Infoln("shutdown complete")
	}()

	// Start pipeline components in the opposite order in which data goes through
	go func() {
		defer wg.Done()
		// Monitor the Batch queue and spawn anchor workers accordingly
		services.NewWorkerService(logger, batchMonitor, jobDb, metricService).Run(serverCtx)
	}()

	dlqConsumer.Start()
	failureConsumer.Start()
	statusConsumer.Start()
	batchingConsumer.Start()
	validationConsumer.Start()

	go func() {
		defer wg.Done()
		// Poll for requests that haven't been processed in time
		services.NewRequestPoller(logger, anchorDb, stateDb, validateQueue, discordHandler).Run(serverCtx)
	}()

	wg.Wait()
}
