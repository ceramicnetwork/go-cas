package queue

import (
	"context"
	"math"
	"sync/atomic"

	"github.com/aws/aws-sdk-go-v2/service/sqs"

	"github.com/abevier/go-sqs/gosqs"

	"github.com/ceramicnetwork/go-cas/models"
)

var _ models.Queue = &multiQueue{}
var _ models.QueuePublisher = &multiQueue{}

type multiQueue struct {
	queues  []models.Queue
	monitor models.QueueMonitor
	index   int32
}

func NewMultiQueue(
	ctx context.Context,
	metricService models.MetricService,
	logger models.Logger,
	sqsClient *sqs.Client,
	opts Opts,
	callback gosqs.MessageCallbackFunc,
	numQueues int,
) (models.Queue, error) {
	// Create at least one sub-queue
	numQueues = int(math.Max(1, float64(numQueues)))
	queues := make([]models.Queue, numQueues)
	monitors := make([]models.QueueMonitor, numQueues)
	if opts.NumWorkers != nil {
		// Divide the workers evenly among the sub-queues
		*opts.NumWorkers = int(math.Ceil(float64(*opts.NumWorkers / numQueues)))
	}
	for i := 0; i < numQueues; i++ {
		if q, _, err := newQueue(ctx, metricService, logger, sqsClient, opts, callback, i); err != nil {
			return nil, err
		} else {
			queues[i] = q
			monitors[i] = q.Monitor()
		}
	}
	return &multiQueue{queues, NewMultiMonitor(monitors), 0}, nil
}

func (m *multiQueue) SendMessage(ctx context.Context, event any) (string, error) {
	// Use an atomic counter to round-robin between sub-queues
	i := atomic.AddInt32(&m.index, 1) - 1
	i = i % int32(len(m.queues))
	return m.queues[i].Publisher().SendMessage(ctx, event)
}

func (m *multiQueue) Start() {
	// Start all sub-queues
	for _, q := range m.queues {
		q.Start()
	}
}

func (m *multiQueue) Shutdown() {
	// Shutdown all sub-queues
	for _, q := range m.queues {
		q.Shutdown()
	}
}

func (m *multiQueue) WaitForRxShutdown() {
	// Wait for all sub-queues to shutdown
	for _, q := range m.queues {
		q.WaitForRxShutdown()
	}
}

func (m *multiQueue) Monitor() models.QueueMonitor {
	return m.monitor
}

func (m *multiQueue) Publisher() models.QueuePublisher {
	return m
}
