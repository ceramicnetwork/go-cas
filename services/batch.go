package services

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/google/uuid"

	"github.com/abevier/tsk/batch"
	"github.com/abevier/tsk/results"

	"github.com/ceramicnetwork/go-cas/models"
)

type BatchingService struct {
	batchPublisher models.QueuePublisher
	batchStore     models.KeyValueRepository
	batcher        *batch.Executor[*models.AnchorRequestMessage, *uuid.UUID]
	metricService  models.MetricService
	logger         models.Logger
	flushTicker    *time.Ticker
	flushInterval  time.Duration
}

func NewBatchingService(ctx context.Context, batchPublisher models.QueuePublisher, batchStore models.KeyValueRepository, metricService models.MetricService, logger models.Logger) *BatchingService {
	anchorBatchSize := models.DefaultAnchorBatchSize
	if configAnchorBatchSize, found := os.LookupEnv(models.Env_AnchorBatchSize); found {
		if parsedAnchorBatchSize, err := strconv.Atoi(configAnchorBatchSize); err == nil {
			anchorBatchSize = parsedAnchorBatchSize
		}
	}
	anchorBatchLinger := models.DefaultAnchorBatchLinger
	if configAnchorBatchLinger, found := os.LookupEnv("ANCHOR_BATCH_LINGER"); found {
		if parsedAnchorBatchLinger, err := time.ParseDuration(configAnchorBatchLinger); err == nil {
			anchorBatchLinger = parsedAnchorBatchLinger
		}
	}
	anchorBatchFlushInterval := time.Hour // Default to 1 hour
	if configAnchorBatchFlushInterval, found := os.LookupEnv("ANCHOR_BATCH_FLUSH_INTERVAL"); found {
		if parsedAnchorBatchFlushInterval, err := time.ParseDuration(configAnchorBatchFlushInterval); err == nil {
			anchorBatchFlushInterval = parsedAnchorBatchFlushInterval
		}
	}
	batchingService := BatchingService{
		batchPublisher: batchPublisher,
		batchStore:     batchStore,
		metricService:  metricService,
		logger:         logger,
		flushInterval:  anchorBatchFlushInterval,
	}
	beOpts := batch.Opts{MaxSize: anchorBatchSize, MaxLinger: anchorBatchLinger}
	batchingService.batcher = batch.New[*models.AnchorRequestMessage, *uuid.UUID](
		beOpts,
		func(anchorReqs []*models.AnchorRequestMessage) ([]results.Result[*uuid.UUID], error) {
			// Close over the passed context
			return batchingService.batch(ctx, anchorReqs)
		},
	)
	batchingService.startFlushTicker(ctx)
	return &batchingService
}

func (b *BatchingService) Batch(ctx context.Context, msgBody string) error {
	anchorReq := new(models.AnchorRequestMessage)
	if err := json.Unmarshal([]byte(msgBody), anchorReq); err != nil {
		return err
	}
	b.metricService.Count(ctx, models.MetricName_BatchIngressRequest, 1)
	b.logger.Debugw("batch: dequeued",
		"req", anchorReq,
	)
	if _, err := b.batcher.Submit(ctx, anchorReq); err != nil {
		return err
	} else {
		return nil
	}
}

func (b *BatchingService) batch(ctx context.Context, anchorReqs []*models.AnchorRequestMessage) ([]results.Result[*uuid.UUID], error) {
	batchSize := len(anchorReqs)
	anchorReqBatch := models.AnchorBatchMessage{
		Id:  uuid.New(),
		Ids: make([]uuid.UUID, batchSize),
	}
	batchResults := make([]results.Result[*uuid.UUID], batchSize)
	for idx, anchorReq := range anchorReqs {
		anchorReqBatch.Ids[idx] = anchorReq.Id
		batchResults[idx] = results.New[*uuid.UUID](&anchorReqBatch.Id, nil)
	}

	// Store the batch to S3 before sending it to the queue
	key := fmt.Sprintf("cas/anchor/batch/%s", anchorReqBatch.Id.String())
	if err := b.batchStore.Store(ctx, key, anchorReqBatch); err != nil {
		b.logger.Errorf("error storing batch: %v, %v", anchorReqBatch.Id, err)
		return nil, err
	}
	b.metricService.Count(ctx, models.MetricName_BatchStored, 1)

	// Send just the batch ID in the message to the queue
	anchorBatchMessage := models.AnchorBatchMessage{
		Id:  anchorReqBatch.Id,
		Ids: []uuid.UUID{},
	}
	if _, err := b.batchPublisher.SendMessage(ctx, anchorBatchMessage); err != nil {
		b.logger.Errorf("error sending message: %v, %v", anchorReqBatch.Id, err)
		return nil, err
	}
	b.metricService.Count(ctx, models.MetricName_BatchCreated, 1)
	b.metricService.Distribution(ctx, models.MetricName_BatchSize, batchSize)
	b.logger.Debugw(
		"batch generated",
		"batch", anchorReqBatch.Id,
	)
	return batchResults, nil
}

func (b *BatchingService) startFlushTicker(ctx context.Context) {
	// Calculate the duration until the next tick
	now := time.Now().UTC()
	nextTick := now.Truncate(b.flushInterval).Add(b.flushInterval)
	tillNextTick := nextTick.Sub(now)

	// Wait for the initial duration before starting the ticker
	time.AfterFunc(tillNextTick, func() {
		b.Flush()
		b.flushTicker = time.NewTicker(b.flushInterval)
		go b.flushLoop(ctx)
	})
}

func (b *BatchingService) flushLoop(ctx context.Context) {
	for {
		select {
		case <-b.flushTicker.C:
			b.Flush()
		case <-ctx.Done():
			b.flushTicker.Stop()
			return
		}
	}
}

// Flush forces the batching service to flush any pending requests, however far along it's gotten in size or expiration.
//
// We're using this in two ways:
//  1. The top of the hour UTC: We want to flush any pending requests at the top of the hour so that C1 nodes can send
//     in their Merkle Tree roots for anchoring right before the top of the hour. This ensures that the gap between the
//     anchor request being sent and it being anchored is predictable and small. Without this logic, the gap could be
//     upto 1 hour, i.e. till the Scheduler builds a new batch and sends it to the CAS.
//  2. At process shutdown: We want to flush any pending requests when the process is shutting down so that we don't
//     lose any in-flight requests. The caller needs to ensure that no more messages are sent to this service for
//     processing once this function is called. Receiving more messages will cause queue workers to wait till the end of
//     the batch expiration if there aren't enough messages to fill the batch.
func (b *BatchingService) Flush() {
	b.batcher.Flush()
}
