package services

import (
	"context"
	"encoding/json"
	"os"
	"strconv"
	"time"

	"github.com/abevier/tsk/batch"
	"github.com/abevier/tsk/results"
	"github.com/ceramicnetwork/go-cas/models"
)

type BatchingService struct {
	batcher    *batch.Executor[*models.CeramicQuery, *models.StreamState]
}

func NewBatchingService(anchorDb models.AnchorRepository) *BatchingService {
	anchorBatchSize := models.DefaultAnchorBatchSize
	anchorBatchLinger := models.DefaultAnchorBatchLinger

    if configAnchorBatchSize, found := os.LookupEnv("ANCHOR_BATCH_SIZE"); found {
        if parsedAnchorBatchSize, err := strconv.Atoi(configAnchorBatchSize); err == nil {
            anchorBatchSize = parsedAnchorBatchSize
		}
	}

	if configAnchorBatchLinger, found := os.LookupEnv("ANCHOR_BATCH_LINGER"); found {
		if parsedAnchorBatchLinger, err := time.ParseDuration(configAnchorBatchLinger); err == nil {
			anchorBatchLinger = parsedAnchorBatchLinger
        }
	}

	beOpts := batch.Opts{MaxSize: anchorBatchSize, MaxLinger: anchorBatchLinger}

	mqBatcherGen := batch.New[*models.CeramicQuery, *models.StreamState](beOpts, func(queries []*models.CeramicQuery) ([]results.Result[*models.StreamState], error) {
			return clients[idx].multiquery(context.Background(), queries)
		
	

	return &BatchingService{batcher}
}

// Status is the Loading service's message handler. It will be invoked for messages received on the Pin queue. The queue
// plumbing takes care of scaling the consumers, batching, etc.
//
// We won't return errors from here, which will cause the status update to be deleted from the originating queue.
// Requests that are not updated from here will get picked up in a subsequent iteration and reprocessed, which is ok.
func (b BatchingService) Batch(ctx context.Context, msgBody string) error {
	anchorReq := new(models.AnchorRequestMessage)
	if err := json.Unmarshal([]byte(msgBody), anchorReq); err != nil {
		return err
	}
	
	// create job for cd manager
	// worker pulls oldest entry from sqs (FIFO), once done it
	return nil
}