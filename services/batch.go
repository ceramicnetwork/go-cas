package services

import (
	"context"
	"encoding/json"
	"log"
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
	batcher        *batch.Executor[*models.AnchorRequestMessage, *uuid.UUID]
	metricService  models.MetricService
}

func NewBatchingService(ctx context.Context, batchPublisher models.QueuePublisher, metricService models.MetricService) *BatchingService {
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
	batchingService := BatchingService{batchPublisher: batchPublisher, metricService: metricService}
	beOpts := batch.Opts{MaxSize: anchorBatchSize, MaxLinger: anchorBatchLinger}
	batchingService.batcher = batch.New[*models.AnchorRequestMessage, *uuid.UUID](
		beOpts,
		func(anchorReqs []*models.AnchorRequestMessage) ([]results.Result[*uuid.UUID], error) {
			// Close over the passed context
			return batchingService.batch(ctx, anchorReqs)
		},
	)
	return &batchingService
}

func (b BatchingService) Batch(ctx context.Context, msgBody string) error {
	anchorReq := new(models.AnchorRequestMessage)
	if err := json.Unmarshal([]byte(msgBody), anchorReq); err != nil {
		return err
	}
	b.metricService.Count(ctx, models.MetricName_BatchIngressRequest, 1)
	if batchId, err := b.batcher.Submit(ctx, anchorReq); err != nil {
		return err
	} else {
		log.Printf("batch: processed request %s in batch %s", anchorReq.Id, batchId)
		return nil
	}
}

func (b BatchingService) batch(ctx context.Context, anchorReqs []*models.AnchorRequestMessage) ([]results.Result[*uuid.UUID], error) {
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
	if _, err := b.batchPublisher.SendMessage(ctx, anchorReqBatch); err != nil {
		log.Printf("batch: failed to send message: %v, %v", anchorReqBatch, err)
		return nil, err
	}
	b.metricService.Count(ctx, models.MetricName_BatchCreated, 1)
	b.metricService.Distribution(ctx, models.MetricName_BatchSize, batchSize)
	log.Printf("batch: generated batch: %v", anchorReqBatch)
	return batchResults, nil
}

func (b BatchingService) Flush() {
	// Flush the current batch however far along it's gotten in size or expiration. The caller needs to ensure that no
	// more messages are sent to this service for processing once this function is called. Receiving more messages will
	// cause workers to wait till the end of the batch expiration if there aren't enough messages to fill the batch.
	b.batcher.Flush()
}
