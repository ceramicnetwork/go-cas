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
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/sqs"

	"github.com/ceramicnetwork/go-cas"
	"github.com/ceramicnetwork/go-cas/common/aws/config"
	"github.com/ceramicnetwork/go-cas/common/aws/ddb"
	"github.com/ceramicnetwork/go-cas/common/aws/queue"
	"github.com/ceramicnetwork/go-cas/common/aws/storage"
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
	s3Client := s3.NewFromConfig(awsCfg, func(o *s3.Options) {
		o.UsePathStyle = true
	})

	stateDb := ddb.NewStateDb(serverCtx, logger, dynamoDbClient)
	jobDb := ddb.NewJobDb(serverCtx, logger, dynamoDbClient)

	casBucket := "ceramic-" + os.Getenv(cas.Env_Env) + "-cas"
	batchStore := storage.NewS3Store(serverCtx, logger, s3Client, casBucket)

	discordHandler, err := notifs.NewDiscordHandler(logger)
	if err != nil {
		logger.Fatalf("error creating discord handler: %v", err)
	}

	metricService, err := metrics.NewMetricService(serverCtx, logger, models.MetricsCallerName)
	if err != nil {
		logger.Fatalf("error creating metric service: %v", err)
	}

	var visibilityTimeout *time.Duration = nil
	if configVisibilityTimeout, found := os.LookupEnv("QUEUE_VISIBILITY_TIMEOUT"); found {
		if parsedVisibilityTimeout, err := time.ParseDuration(configVisibilityTimeout); err == nil {
			visibilityTimeout = &parsedVisibilityTimeout
		}
	}

	// TODO: Rename queues/services as per the flow below
	// Ref: https://linear.app/3boxlabs/issue/WS1-1586/rename-queuesservices-for-better-readability
	//
	// Data flow through the queues and services:
	// - CAS API posts to the StreamConsolidate queue.
	// - The StreamConsolidation service reads from the StreamConsolidate queue and posts to the ReadyRequest queue(s)
	//   or RequestStatus queue (when a request needs to be marked "Replaced" for some reason).
	// - The RequestBatching service reads from the ReadyRequest queue(s) and collects them into a batch, which it then posts to the
	//   RequestBatch queue. For the duration that the RequestBatching service holds the requests it is batching, it will
	//   keep the SQS messages corresponding to these requests in-flight.
	// - Anchor workers read from the RequestBatch queue and anchor the requests.
	// - The RequestBatch queue is also monitored by the Worker service, which spawns workers to process the batch(es).
	// - The RequestStatus service reads from the RequestStatus queue and updates the Anchor DB.
	// - The Failure handling service reads from the Failure and Dead-Letter queues and posts alerts to Discord.

	// Set up all the services first, so we can create the queues and configure their callbacks. After that, we'll plumb
	// the queues to the right services and start them.
	//
	// The Failure handling service reads from the Failure and Dead-Letter queues
	failureHandlingService := services.NewFailureHandlingService(discordHandler, metricService, logger)
	// The Validation service reads from the Validate queue and posts to the Ready and Status queues
	validationService := services.NewValidationService(stateDb, metricService, logger)
	// The Batching service reads from the Ready queue(s) and posts to the Batch queue.
	batchingService := services.NewBatchingService(serverCtx, batchStore, metricService, logger)
	// The Status service reads from the Status queue and updates the Anchor DB
	statusService := services.NewStatusService(anchorDb, metricService, logger)

	// Now create all the queues.
	//
	// Create the DLQ and prepare the redrive options for the other queues
	deadLetterQueue, dlqId, err := queue.NewQueue(
		serverCtx,
		metricService,
		logger,
		sqsClient,
		queue.Opts{QueueType: queue.Type_DLQ, VisibilityTimeout: visibilityTimeout},
		failureHandlingService.DLQ,
	)
	if err != nil {
		logger.Fatalf("error creating dead-letter queue: %v", err)
	}
	redriveOpts := &queue.RedriveOpts{
		DlqId:           dlqId,
		MaxReceiveCount: queue.DefaultMaxReceiveCount,
	}
	// Failure queue
	// TODO: Could this become recursive since the failure handler also consumes from the DLQ? The inability to handle
	// failures could put messages back in the DLQ that are then re-consumed by the handler.
	failureQueue, _, err := queue.NewQueue(
		serverCtx,
		metricService,
		logger,
		sqsClient,
		queue.Opts{
			QueueType:         queue.Type_Failure,
			VisibilityTimeout: visibilityTimeout,
			RedriveOpts:       redriveOpts,
		},
		failureHandlingService.Failure,
	)
	if err != nil {
		logger.Fatalf("error creating failure queue: %v", err)
	}
	// Validate queue
	validateQueue, _, err := queue.NewQueue(
		serverCtx,
		metricService,
		logger,
		sqsClient,
		queue.Opts{
			QueueType:         queue.Type_Validate,
			VisibilityTimeout: visibilityTimeout,
			RedriveOpts:       redriveOpts,
		},
		validationService.Validate,
	)
	if err != nil {
		logger.Fatalf("error creating validate queue: %v", err)
	}
	// The Ready and Batch queues will need larger visibility timeouts than the other queues. Requests pulled from the
	// Ready queue will remain in flight for the batch linger duration. Batches from the Batch queue will remain in
	// flight as long as it takes for them to get anchored.
	//
	// These queues will thus allow a smaller maximum receive count before messages fall through to the DLQ. Detecting
	// failures is harder given the longer visibility timeouts, so it's important that they be detected as soon as
	// possible.
	anchorBatchLinger := models.DefaultAnchorBatchLinger
	if configAnchorBatchLinger, found := os.LookupEnv("ANCHOR_BATCH_LINGER"); found {
		if parsedAnchorBatchLinger, err := time.ParseDuration(configAnchorBatchLinger); err == nil {
			anchorBatchLinger = parsedAnchorBatchLinger
		}
	}
	// Add one hour to the anchor batch linger to get the long queue visibility timeout
	longQueueVisibilityTimeout := anchorBatchLinger + time.Hour
	longQueueMaxReceiveCount := redriveOpts.MaxReceiveCount
	if configMaxReceiveCount, found := os.LookupEnv("LONG_QUEUE_MAX_RECEIVE_COUNT"); found {
		if parsedMaxReceiveCount, err := strconv.Atoi(configMaxReceiveCount); err == nil {
			longQueueMaxReceiveCount = parsedMaxReceiveCount
		}
	}
	longQueueRedriveOpts := &queue.RedriveOpts{
		DlqId:           dlqId,
		MaxReceiveCount: longQueueMaxReceiveCount,
	}
	anchorBatchSize := models.DefaultAnchorBatchSize
	if configAnchorBatchSize, found := os.LookupEnv(models.Env_AnchorBatchSize); found {
		if parsedAnchorBatchSize, err := strconv.Atoi(configAnchorBatchSize); err == nil {
			anchorBatchSize = parsedAnchorBatchSize
		}
	}
	// Ready queue
	//
	// Create a minimum of 10 Ready queue publishers, or as many needed to process an anchor batch while keeping each
	// queue below the maximum number of inflight SQS messages (120,000).
	numReadyPublishers := int(math.Max(10, float64(anchorBatchSize/120_000)))
	// The Ready queue is a "multi-queue", which is necessary because of the unconventional way we use it.
	//
	// The BatchingService contains logic to "hold" anchor request SQS messages until one of two conditions is
	// satisfied: either the batch linger duration has run out, or the batch is full. While requests are held this way,
	// they are considered "in-flight" w.r.t. SQS. Once a batch is formed and recorded, all the messages that comprise
	// the batch are ACK'd and thus deleted from SQS. If a batch fails to get created, all messages will be NACK'd (if
	// there is a handled error) or simply not ACK'd (if there is an unhandled error / crash), which will make them
	// visible to the BatchingService once it recovers/retries.
	//
	// This allows SQS to be the persistence for messages flowing through the system versus needing to store batches in
	// a DB to be able to recover messages in case of a failure. While the latter is doable, it pushes the (non-trivial)
	// complexity for maintaining batches down to the DB.
	//
	// Given this context and the fact that the maximum number of in-flight message a SQS queue can have is 120,000, we
	// can only have batches of up to 120,000 requests with a single queue. Using a multi-queue allows the
	// BatchingService to hold a much larger number of anchor requests in-flight, up to 120,000 per sub-queue. This
	// allows batches to be constructed with virtually any size that we want.
	//
	// Because each message is held in-flight till its batch is formed, we need a number of consumer workers greater
	// than the batch size. This prevents a smaller number of workers from waiting on an incomplete batch to fill up
	// because there aren't any workers available to add to the batch even when messages are available in the queue. The
	// 2 multiplier is arbitrary but will allow two batches worth of requests to be read and processed in parallel.
	maxReadyQueueWorkers := anchorBatchSize * 2
	readyQueue, err := queue.NewMultiQueue(
		serverCtx,
		metricService,
		logger,
		sqsClient,
		queue.Opts{
			QueueType:         queue.Type_Ready,
			VisibilityTimeout: &longQueueVisibilityTimeout,
			RedriveOpts:       longQueueRedriveOpts,
			NumWorkers:        &maxReadyQueueWorkers,
		},
		batchingService.Batch,
		numReadyPublishers,
	)
	if err != nil {
		logger.Fatalf("error creating ready queue: %v", err)
	}
	// Batch queue
	batchQueue, _, err := queue.NewQueue(
		serverCtx,
		metricService,
		logger,
		sqsClient,
		queue.Opts{
			QueueType:         queue.Type_Batch,
			VisibilityTimeout: &longQueueVisibilityTimeout,
			RedriveOpts:       longQueueRedriveOpts,
		},
		nil,
	)
	if err != nil {
		logger.Fatalf("error creating batch queue: %v", err)
	}
	// Status queue
	statusQueue, _, err := queue.NewQueue(
		serverCtx,
		metricService,
		logger,
		sqsClient,
		queue.Opts{
			QueueType:         queue.Type_Status,
			VisibilityTimeout: visibilityTimeout,
			RedriveOpts:       redriveOpts,
		},
		statusService.Status,
	)
	if err != nil {
		logger.Fatalf("error creating status queue: %v", err)
	}

	// Wire up the queues and services, then start the services.
	validationService.Start(readyQueue.Publisher(), statusQueue.Publisher())
	batchingService.Start(batchQueue.Publisher())

	wg := sync.WaitGroup{}
	wg.Add(2)

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
		validateQueue.Shutdown()

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
			readyQueue.Shutdown()
		}()
		go func() {
			defer batchWg.Done()
			readyQueue.WaitForRxShutdown()
			time.Sleep(5 * time.Second)
			batchingService.Flush()
		}()
		batchWg.Wait()

		statusQueue.Shutdown()
		failureQueue.Shutdown()
		deadLetterQueue.Shutdown()

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
		services.NewWorkerService(logger, batchQueue.Monitor(), jobDb, metricService).Run(serverCtx)
	}()

	deadLetterQueue.Start()
	failureQueue.Start()
	statusQueue.Start()
	readyQueue.Start()
	validateQueue.Start()

	if configAnchorAuditEnabled, found := os.LookupEnv(models.Env_AnchorAuditEnabled); found {
		if anchorAuditEnabled, err := strconv.ParseBool(configAnchorAuditEnabled); (err == nil) && anchorAuditEnabled {
			wg.Add(1)
			go func() {
				defer wg.Done()
				// Enable auditing of the anchor DB to check for pending anchor requests that might have been missed
				services.NewRequestPoller(logger, anchorDb, stateDb, validateQueue.Publisher(), discordHandler).Run(serverCtx)
			}()
		}
	}

	wg.Wait()
}
