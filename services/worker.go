package services

import (
	"context"
	"log"
	"math"
	"os"
	"strconv"
	"time"

	"github.com/ceramicnetwork/go-cas/models"
)

type WorkerService struct {
	batchMonitor     models.QueueMonitor
	jobDb            models.JobRepository
	monitorTick      time.Duration
	maxAnchorWorkers int
}

func NewWorkerService(batchMonitor models.QueueMonitor, jobDb models.JobRepository) *WorkerService {
	batchMonitorTick := models.DefaultAnchorBatchMonitorTick
	if configBatchMonitorTick, found := os.LookupEnv("ANCHOR_BATCH_MONITOR_TICK"); found {
		if parsedBatchMonitorTick, err := time.ParseDuration(configBatchMonitorTick); err == nil {
			batchMonitorTick = parsedBatchMonitorTick
		}
	}
	maxAnchorWorkers := models.DefaultMaxAnchorWorkers
	if configMaxAnchorWorkers, found := os.LookupEnv("MAX_ANCHOR_WORKERS"); found {
		if parsedMaxAnchorWorkers, err := strconv.Atoi(configMaxAnchorWorkers); err == nil {
			maxAnchorWorkers = parsedMaxAnchorWorkers
		}
	}
	return &WorkerService{batchMonitor, jobDb, batchMonitorTick, maxAnchorWorkers}
}

func (w WorkerService) Run(ctx context.Context) {
	log.Printf("worker: started")
	tick := time.NewTicker(w.monitorTick)
	for {
		select {
		case <-ctx.Done():
			log.Printf("worker: stopped")
			return
		case <-tick.C:
			numJobsCreated, err := w.launch(ctx)
			log.Printf("worker: created %d jobs, error = %v", numJobsCreated, err)
		}
	}
}

func (w WorkerService) launch(ctx context.Context) (int, error) {
	if numBatchesUnprocessed, numBatchesInFlight, err := w.batchMonitor.GetQueueUtilization(ctx); err != nil {
		return 0, err
	} else {
		// The number of unprocessed batches can be used to determine the ingress load on the anchoring system.
		//
		// The number of batches in flight can be used to observe the current state of the system. Each anchor worker
		// will process a single batch at a time, even if it continues to poll the queue for more batches as it gets
		// done with previous ones. This means that we can infer the number of running workers by looking at the number
		// of batches in flight.
		numJobsAllowed := 0
		if w.maxAnchorWorkers == -1 {
			// We can create as many workers as needed to service unprocessed batches
			numJobsAllowed = numBatchesUnprocessed
		} else if numBatchesInFlight < w.maxAnchorWorkers {
			// We can create workers upto the maximum allowed minus the number already running
			numJobsAllowed = w.maxAnchorWorkers - numBatchesInFlight
		}
		numJobsToCreate := int(math.Min(float64(numJobsAllowed), float64(numBatchesUnprocessed)))
		var numJobsCreated int
		for numJobsCreated = 0; numJobsCreated < numJobsToCreate; numJobsCreated++ {
			if err = w.jobDb.CreateJob(ctx); err != nil {
				break
			}
		}
		return numJobsCreated, err
	}
}
