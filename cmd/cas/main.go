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
		logger.Fatalf("main: error creating aws cfg: %v", err)
	}

	anchorDb := db.NewAnchorDb(logger)

	// HTTP clients
	dynamoDbClient := dynamodb.NewFromConfig(awsCfg)
	sqsClient := sqs.NewFromConfig(awsCfg)

	stateDb := ddb.NewStateDb(serverCtx, logger, dynamoDbClient)
	jobDb := ddb.NewJobDb(serverCtx, logger, dynamoDbClient)

	discordHandler, err := notifs.NewDiscordHandler(logger)
	if err != nil {
		logger.Fatalf("main: failed to create discord handler: %v", err)
	}

	metricService, err := metrics.NewMetricService(serverCtx, logger)
	if err != nil {
		logger.Fatalf("main: failed to create metric service: %v", err)
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
		logger.Fatalf("main: failed to create dead-letter queue: %v", err)
	}
	dlqArn, err := queue.GetQueueArn(serverCtx, deadLetterQueue.GetUrl(), sqsClient)
	if err != nil {
		logger.Fatalf("main: failed to fetch dead-letter queue arn: %v", err)
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
		logger.Fatalf("main: failed to create failure queue: %v", err)
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
		logger.Fatalf("main: failed to create validate queue: %v", err)
	}
	// Ready queue
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
		logger.Fatalf("main: failed to create ready queue: %v", err)
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
		logger.Fatalf("main: failed to create batch queue: %v", err)
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
		logger.Fatalf("main: failed to create status queue: %v", err)
	}

	// Create utilization gauges for all the queues
	if err = metricService.QueueGauge(serverCtx, deadLetterQueue.GetName(), queue.NewMonitor(deadLetterQueue.GetUrl(), sqsClient)); err != nil {
		logger.Fatalf("main: failed to create utilization gauge for dead-letter queue: %v", err)
	}
	if err = metricService.QueueGauge(serverCtx, failureQueue.GetName(), queue.NewMonitor(failureQueue.GetUrl(), sqsClient)); err != nil {
		logger.Fatalf("main: failed to create utilization gauge for failure queue: %v", err)
	}
	if err = metricService.QueueGauge(serverCtx, validateQueue.GetName(), queue.NewMonitor(validateQueue.GetUrl(), sqsClient)); err != nil {
		logger.Fatalf("main: failed to create utilization gauge for validate queue: %v", err)
	}
	if err = metricService.QueueGauge(serverCtx, readyQueue.GetName(), queue.NewMonitor(readyQueue.GetUrl(), sqsClient)); err != nil {
		logger.Fatalf("main: failed to create utilization gauge for ready queue: %v", err)
	}
	batchMonitor := queue.NewMonitor(batchQueue.GetUrl(), sqsClient)
	if err = metricService.QueueGauge(serverCtx, batchQueue.GetName(), batchMonitor); err != nil {
		logger.Fatalf("main: failed to create utilization gauge for batch queue: %v", err)
	}
	if err = metricService.QueueGauge(serverCtx, statusQueue.GetName(), queue.NewMonitor(statusQueue.GetUrl(), sqsClient)); err != nil {
		logger.Fatalf("main: failed to create utilization gauge for status queue: %v", err)
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
	wg.Add(2)

	// Set up graceful shutdown
	go func() {
		defer wg.Done()

		interruptCh := make(chan os.Signal, 1)
		signal.Notify(interruptCh, syscall.SIGTERM)
		<-interruptCh
		logger.Infoln("main: shutdown started")

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

		logger.Infoln("main: shutdown complete")
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

	wg.Wait()
}
