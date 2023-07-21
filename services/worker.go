package services

import (
	"context"
	"math"
	"os"
	"strconv"
	"time"

	"github.com/ceramicnetwork/go-cas/models"
)

const defaultAnchorBatchMonitorTick = 5 * time.Minute
const defaultMaxAnchorWorkers = 1

type WorkerService struct {
	batchMonitor     models.QueueMonitor
	jobDb            models.JobRepository
	metricService    models.MetricService
	monitorTick      time.Duration
	maxAnchorWorkers int
	anchorJobs       map[string]*models.JobState
	logger           models.Logger
}

func NewWorkerService(logger models.Logger, batchMonitor models.QueueMonitor, jobDb models.JobRepository, metricService models.MetricService) *WorkerService {
	batchMonitorTick := defaultAnchorBatchMonitorTick
	if configBatchMonitorTick, found := os.LookupEnv("ANCHOR_BATCH_MONITOR_TICK"); found {
		if parsedBatchMonitorTick, err := time.ParseDuration(configBatchMonitorTick); err == nil {
			batchMonitorTick = parsedBatchMonitorTick
		}
	}
	maxAnchorWorkers := defaultMaxAnchorWorkers
	if configMaxAnchorWorkers, found := os.LookupEnv("MAX_ANCHOR_WORKERS"); found {
		if parsedMaxAnchorWorkers, err := strconv.Atoi(configMaxAnchorWorkers); err == nil {
			maxAnchorWorkers = parsedMaxAnchorWorkers
		}
	}
	return &WorkerService{
		batchMonitor,
		jobDb,
		metricService,
		batchMonitorTick,
		maxAnchorWorkers,
		make(map[string]*models.JobState),
		logger,
	}
}

func (w WorkerService) Run(ctx context.Context) {
	w.logger.Infof("worker: started")
	tick := time.NewTicker(w.monitorTick)
	for {
		select {
		case <-ctx.Done():
			w.logger.Infof("worker: stopped")
			return
		case <-tick.C:
			numJobsCreated, err := w.launch(ctx)
			w.logger.Infof("worker: created %d jobs, error = %v", numJobsCreated, err)
		}
	}
}

func (w WorkerService) launch(ctx context.Context) (int, error) {
	if numJobsRequired, numExistingJobs, err := w.calculateScaling(ctx); err != nil {
		return 0, err
	} else {
		numJobsAllowed := 0
		if w.maxAnchorWorkers == -1 {
			// We can create as many workers as needed to service unprocessed batches
			numJobsAllowed = numJobsRequired
		} else if numExistingJobs < w.maxAnchorWorkers {
			// We can create workers upto the maximum allowed minus the number of jobs already created
			numJobsAllowed = w.maxAnchorWorkers - numExistingJobs
		}
		numJobsToCreate := int(math.Min(float64(numJobsAllowed), float64(numJobsRequired)))
		var numJobsCreated int
		for numJobsCreated = 0; numJobsCreated < numJobsToCreate; numJobsCreated++ {
			if jobId, err := w.jobDb.CreateJob(ctx); err != nil {
				break
			} else {
				w.anchorJobs[jobId] = nil
			}
		}
		w.logger.Debugf("worker: numJobsRequired=%d, anchorJobs=%v", numJobsRequired, w.anchorJobs)
		w.metricService.Count(ctx, models.MetricName_WorkerJobCreated, numJobsCreated)
		return numJobsCreated, err
	}
}

func (w WorkerService) calculateScaling(ctx context.Context) (int, int, error) {
	// Clean up finished jobs
	for jobId, _ := range w.anchorJobs {
		if jobState, err := w.jobDb.QueryJob(ctx, jobId); err != nil {
			return 0, 0, err
		} else if (jobState.Stage == models.JobStage_Completed) || (jobState.Stage == models.JobStage_Failed) {
			// Clean out finished jobs - "completed" and "failed" are the only possible terminal stages for anchor jobs
			delete(w.anchorJobs, jobId)
		} else {
			w.anchorJobs[jobId] = jobState
		}
	}
	// This includes jobs that have been created but not yet started (perhaps due to a deployment in progress). If there
	// is a problem starting jobs on the CD manager or anchor worker side, we won't keep creating new jobs unless we
	// have newer batches to process.
	//
	// Alerting on the size of the batch queue backlog will inform us if jobs haven't been making progress while batches
	// have continued to be created.
	numExistingJobs := len(w.anchorJobs)
	if numBatchesUnprocessed, _, err := w.batchMonitor.GetUtilization(ctx); err != nil {
		return 0, 0, err
	} else {
		// The number of active workers can be used to observe the current throughput of the system. Each anchor worker
		// will process a single batch at a time, even if it continues to poll the queue for more batches as it gets
		// done with earlier ones.
		//
		// The number of overflow batches (i.e. the surplus over the number of active workers) can be used to determine
		// the additional ingress load on the anchoring system and thus the number of new workers required.
		return int(math.Max(float64(numBatchesUnprocessed-numExistingJobs), 0)), numExistingJobs, nil
	}
}
