package services

import (
	"context"
	"math"
	"os"
	"strconv"
	"time"

	"github.com/3box/pipeline-tools/cd/manager/common/job"

	"github.com/ceramicnetwork/go-cas/models"
)

const defaultAnchorBatchMonitorTick = 30 * time.Second
const defaultMaxAnchorWorkers = 1
const defaultAmortizationFactor = 1.0

type WorkerService struct {
	batchMonitor       models.QueueMonitor
	jobDb              models.JobRepository
	metricService      models.MetricService
	monitorTick        time.Duration
	maxAnchorWorkers   int
	amortizationFactor float64
	anchorJobs         map[string]*job.JobState
	logger             models.Logger
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
	amortizationFactor := defaultAmortizationFactor
	if configAmortizationFactor, found := os.LookupEnv("ANCHOR_WORKER_AMORTIZATION"); found {
		if parsedAmortizationFactor, err := strconv.ParseFloat(configAmortizationFactor, 64); err == nil {
			amortizationFactor = parsedAmortizationFactor
		}
	}
	return &WorkerService{
		batchMonitor,
		jobDb,
		metricService,
		batchMonitorTick,
		maxAnchorWorkers,
		amortizationFactor,
		make(map[string]*job.JobState),
		logger,
	}
}

func (w WorkerService) Run(ctx context.Context) {
	w.logger.Infof("started")
	tick := time.NewTicker(w.monitorTick)
	for {
		select {
		case <-ctx.Done():
			w.logger.Infof("stopped")
			return
		case <-tick.C:
			if err := w.createJobs(ctx); err != nil {
				w.logger.Errorf("error creating jobs: %v", err)
			}
		}
	}
}

func (w WorkerService) createJobs(ctx context.Context) error {
	if numJobsRequired, numExistingJobs, err := w.calculateLoad(ctx); err != nil {
		return err
	} else {
		numJobsAllowed := 0
		if w.maxAnchorWorkers == -1 {
			// We can create as many workers as needed to service unprocessed batches
			numJobsAllowed = numJobsRequired
		} else if numExistingJobs < w.maxAnchorWorkers {
			// We can create workers upto the maximum allowed minus the number of jobs already created
			numJobsAllowed = w.maxAnchorWorkers - numExistingJobs
		}
		amortizedNumJobsAllowed := math.Ceil(float64(numJobsAllowed) * w.amortizationFactor)
		numJobsToCreate := int(math.Min(amortizedNumJobsAllowed, float64(numJobsRequired)))
		var numJobsCreated int
		for numJobsCreated = 0; numJobsCreated < numJobsToCreate; numJobsCreated++ {
			if jobId, err := w.jobDb.CreateJob(ctx); err != nil {
				break
			} else {
				w.anchorJobs[jobId] = nil
			}
		}
		w.logger.Debugw(
			"job counts",
			"numJobsRequired", numJobsRequired,
			"numExistingJobs", numExistingJobs,
			"numJobsAllowed", numJobsAllowed,
			"amortizedNumJobsAllowed", amortizedNumJobsAllowed,
			"numJobsToCreate", numJobsToCreate,
			"numJobsCreated", numJobsCreated,
			"anchorJobs", w.anchorJobs,
		)
		w.metricService.Count(ctx, models.MetricName_WorkerJobCreated, numJobsCreated)
		return err
	}
}

func (w WorkerService) calculateLoad(ctx context.Context) (int, int, error) {
	// Clean up finished jobs
	for jobId, _ := range w.anchorJobs {
		if jobState, err := w.jobDb.QueryJob(ctx, jobId); err != nil {
			return 0, 0, err
		} else if (jobState.Stage == job.JobStage_Completed) || (jobState.Stage == job.JobStage_Failed) {
			// Clean out finished jobs - "completed" and "failed" are the only possible terminal stages for anchor jobs.
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
	if numBatchesUnprocessed, numBatchesInflight, err := w.batchMonitor.GetUtilization(ctx); err != nil {
		return 0, 0, err
	} else {
		// The total number of unprocessed and inflight batches can be used to observe the current load on the system
		// and thus to determine how many jobs need to be created to handle this load.
		//
		// Each anchor worker will process a single batch at a time, even if it continues to poll the queue for more
		// batches as it gets done with earlier ones.
		return int(math.Max(float64(numBatchesUnprocessed+numBatchesInflight-numExistingJobs), 0)), numExistingJobs, nil
	}
}
