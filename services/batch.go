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

func NewBatchingService(batchPublisher models.QueuePublisher, metricService models.MetricService) *BatchingService {
	anchorBatchSize := models.DefaultAnchorBatchSize
	if configAnchorBatchSize, found := os.LookupEnv("ANCHOR_BATCH_SIZE"); found {
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
	batchingService.batcher = batch.New[*models.AnchorRequestMessage, *uuid.UUID](beOpts, batchingService.batch)
	return &batchingService
}

func (b BatchingService) Batch(ctx context.Context, msgBody string) error {
	anchorReq := new(models.AnchorRequestMessage)
	if err := json.Unmarshal([]byte(msgBody), anchorReq); err != nil {
		return err
	}
	if batchId, err := b.batcher.Submit(ctx, anchorReq); err != nil {
		return err
	} else {
		log.Printf("batch: processed request %s in batch %s", anchorReq.Id, batchId)
		return nil
	}
}

func (b BatchingService) batch(anchorReqs []*models.AnchorRequestMessage) ([]results.Result[*uuid.UUID], error) {
	anchorReqBatch := models.AnchorBatchMessage{
		Id:  uuid.New(),
		Ids: make([]uuid.UUID, len(anchorReqs)),
	}
	batchResults := make([]results.Result[*uuid.UUID], len(anchorReqs))
	for idx, anchorReq := range anchorReqs {
		anchorReqBatch.Ids[idx] = anchorReq.Id
		batchResults[idx] = results.New[*uuid.UUID](&anchorReqBatch.Id, nil)
	}
	if _, err := b.batchPublisher.SendMessage(context.Background(), anchorReqBatch); err != nil {
		log.Printf("batch: failed to send message: %v, %v", anchorReqBatch, err)
		return nil, err
	}
	b.metricService.Count(context.Background(), models.CreatedBatchMetricName, 1)
	log.Printf("batch: generated batch: %v", anchorReqBatch)
	return batchResults, nil
}
