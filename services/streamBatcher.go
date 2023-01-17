package services

import (
	"context"
	"encoding/json"
	"go/types"
	"os"
	"strconv"
	"time"

	"github.com/abevier/tsk/batch"
	"github.com/abevier/tsk/results"

	"github.com/smrz2001/go-cas/models"
)

const DefaultMaxAnchoringDelay = 12 * time.Hour
const DefaultMinStreamCount = 1024

type BatchingService struct {
	workerPublisher  queuePublisher
	failurePublisher queuePublisher
	batcher          *batch.Executor[*models.StreamReadyMessage, types.Nil]
}

func NewBatchingService(workerPublisher, failurePublisher queuePublisher) *BatchingService {
	streamBatcher := BatchingService{}

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
	batcher := batch.New[*models.StreamReadyMessage, types.Nil](beOpts, streamBatcher.batch)
	return &BatchingService{
		workerPublisher,
		failurePublisher,
		batcher,
	}
}

func (b BatchingService) ProcessReady(ctx context.Context, msgBody string) error {
	readyEvent := new(models.StreamReadyMessage)
	err := json.Unmarshal([]byte(msgBody), readyEvent)
	if err != nil {
		return err
	}
	if _, err = b.batcher.Submit(ctx, readyEvent); err != nil {
		return err
	}
	return nil
}

func (b BatchingService) batch(readyEvents []*models.StreamReadyMessage) ([]results.Result[types.Nil], error) {
	workerMessage := models.AnchorWorkerMessage{StreamIds: make([]string, len(readyEvents))}
	for idx, readyEvent := range readyEvents {
		workerMessage.StreamIds[idx] = readyEvent.StreamId
	}
	res := make([]results.Result[types.Nil], len(readyEvents))
	if _, err := b.workerPublisher.SendMessage(context.Background(), workerMessage); err != nil {
		return nil, err
	}
	return res, nil
}
