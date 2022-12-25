package batch

import (
	"context"
	"encoding/json"
	"go/types"
	"log"
	"os"
	"strconv"
	"time"

	"github.com/abevier/tsk/batch"
	"github.com/abevier/tsk/results"

	"github.com/aws/aws-sdk-go-v2/service/sqs"

	"github.com/abevier/go-sqs/gosqs"

	"github.com/smrz2001/go-cas/aws"
	"github.com/smrz2001/go-cas/models"
)

const DefaultMaxAnchoringDelay = 12 * time.Hour
const DefaultMinStreamCount = 1024

type BatchingService struct {
	readyConsumer    *gosqs.SQSConsumer
	workerPublisher  *gosqs.SQSPublisher
	failurePublisher *gosqs.SQSPublisher
	batcher          *batch.BatchExecutor[*models.StreamReadyEvent, types.Nil]
}

func NewBatchingService() *BatchingService {
	cfg, err := cas.AwsConfig()
	if err != nil {
		log.Fatalf("newStreamBatcher: error creating aws cfg: %v", err)
	}
	streamBatcher := BatchingService{}
	client := sqs.NewFromConfig(cfg)
	qOpts := gosqs.Opts{
		MaxReceivedMessages:               models.DefaultMaxReceivedMessages,
		MaxWorkers:                        models.DefaultMaxNumWorkers,
		MaxInflightReceiveMessageRequests: models.DefaultMaxInflightMessages,
	}
	// Ready queue:
	// - Loader publishes
	// - Batcher consumes
	readyPublisher := gosqs.NewPublisher(
		client,
		cas.GetQueueUrl(client, models.QueueType_Ready),
		models.DefaultBatchMaxLinger,
	)
	readyConsumer := gosqs.NewConsumer(qOpts, readyPublisher, streamBatcher.processReady)
	// Worker queue:
	// - Batcher publishes
	// - Anchor worker consumes
	workerPublisher := gosqs.NewPublisher(
		client,
		cas.GetQueueUrl(client, models.QueueType_Worker),
		models.DefaultBatchMaxLinger,
	)
	// Failure queue:
	// - All services publish
	// - Failure handler consumes
	failurePublisher := gosqs.NewPublisher(
		client,
		cas.GetQueueUrl(client, models.QueueType_Failure),
		models.DefaultBatchMaxLinger,
	)
	maxAnchoringDelay := DefaultMaxAnchoringDelay
	if anchoringDelayEnv, found := os.LookupEnv("CAS_MAX_ANCHORING_DELAY"); found {
		if parsedAnchoringDelay, err := time.ParseDuration(anchoringDelayEnv); err == nil {
			maxAnchoringDelay = parsedAnchoringDelay
		}
	}
	minStreamCount := DefaultMinStreamCount
	if minStreamCountEnv, found := os.LookupEnv("CAS_MIN_STREAM_COUNT"); found {
		if parsedMinStreamCount, err := strconv.Atoi(minStreamCountEnv); err == nil {
			minStreamCount = parsedMinStreamCount
		}
	}
	beOpts := batch.Opts{MaxSize: minStreamCount, MaxLinger: maxAnchoringDelay}
	batcher := batch.New[*models.StreamReadyEvent, types.Nil](beOpts, streamBatcher.batch)
	return &BatchingService{
		readyConsumer,
		workerPublisher,
		failurePublisher,
		batcher,
	}
}

func (b BatchingService) processReady(ctx context.Context, msgBody string) error {
	readyEvent := new(models.StreamReadyEvent)
	err := json.Unmarshal([]byte(msgBody), readyEvent)
	if err != nil {
		return err
	}
	if _, err = b.batcher.Submit(ctx, readyEvent); err != nil {
		return err
	}
	return nil
}

func (b BatchingService) batch(readyEvents []*models.StreamReadyEvent) ([]results.Result[types.Nil], error) {
	workerEvent := models.AnchorWorkerEvent{StreamIds: make([]string, len(readyEvents))}
	for idx, readyEvent := range readyEvents {
		workerEvent.StreamIds[idx] = readyEvent.StreamId
	}
	res := make([]results.Result[types.Nil], len(readyEvents))
	if _, err := cas.PublishEvent(context.Background(), b.workerPublisher, workerEvent); err != nil {
		return nil, err
	}
	return res, nil
}
