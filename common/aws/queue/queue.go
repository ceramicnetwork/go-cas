package queue

import (
	"context"
	"encoding/json"
	"math"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/sqs"

	"github.com/abevier/go-sqs/gosqs"

	"github.com/ceramicnetwork/go-cas/models"
)

var _ models.Queue = &queue{}
var _ models.QueuePublisher = &queue{}

const maxLinger = 250 * time.Millisecond
const defaultNumConsumerWorkers = 1000

type RedriveOpts struct {
	DlqId           string
	MaxReceiveCount int
}

type Opts struct {
	QueueType         Type
	VisibilityTimeout *time.Duration
	RedriveOpts       *RedriveOpts
	NumWorkers        *int
}

type queue struct {
	queueType Type
	index     int
	publisher *gosqs.SQSPublisher
	consumer  *gosqs.SQSConsumer
	monitor   models.QueueMonitor
	logger    models.Logger
}

func NewQueue(
	ctx context.Context,
	metricService models.MetricService,
	logger models.Logger,
	sqsClient *sqs.Client,
	opts Opts,
	callback gosqs.MessageCallbackFunc,
) (models.Queue, string, error) {
	return newQueue(ctx, metricService, logger, sqsClient, opts, callback, 0)
}

func newQueue(
	ctx context.Context,
	metricService models.MetricService,
	logger models.Logger,
	sqsClient *sqs.Client,
	opts Opts,
	callback gosqs.MessageCallbackFunc,
	index int,
) (models.Queue, string, error) {
	// Create the queue if it didn't already exist
	if url, arn, name, err := CreateQueue(ctx, sqsClient, opts, index); err != nil {
		return nil, "", err
	} else {
		monitor := NewMonitor(url, sqsClient)
		if err = metricService.QueueGauge(ctx, name, monitor); err != nil {
			logger.Fatalf("error creating gauge for %s queue: %v", name, err)
		}
		publisher := gosqs.NewPublisher(
			sqsClient,
			url,
			maxLinger,
		)
		var maxWorkers float64 = defaultNumConsumerWorkers
		if opts.NumWorkers != nil {
			// Don't go below the default number of workers
			maxWorkers = math.Max(maxWorkers, float64(*opts.NumWorkers))
		}
		maxReceivedMessages := math.Ceil(float64(maxWorkers) * 1.2)
		maxInflightRequests := math.Ceil(maxReceivedMessages / 10)
		qOpts := gosqs.Opts{
			MaxReceivedMessages:               int(maxReceivedMessages),
			MaxWorkers:                        int(maxWorkers),
			MaxInflightReceiveMessageRequests: int(maxInflightRequests),
		}
		return &queue{
			opts.QueueType,
			index,
			publisher,
			gosqs.NewConsumer(qOpts, publisher, callback),
			monitor,
			logger,
		}, arn, nil
	}
}

func (p queue) SendMessage(ctx context.Context, event any) (string, error) {
	if eventBody, err := json.Marshal(event); err != nil {
		return "", err
	} else if msgId, err := p.publisher.SendMessage(ctx, string(eventBody)); err != nil {
		return "", err
	} else {
		return msgId, nil
	}
}

func (p queue) Start() {
	p.consumer.Start()
	p.logger.Infof("%s: started", p.queueType)
}

func (p queue) Shutdown() {
	p.consumer.Shutdown()
	p.logger.Infof("%s: stopped", p.queueType)
}

func (p queue) WaitForRxShutdown() {
	p.consumer.WaitForRxShutdown()
	p.logger.Infof("%s: rx stopped", p.queueType)
}

func (p queue) Monitor() models.QueueMonitor {
	return p.monitor
}

func (p queue) Publisher() models.QueuePublisher {
	return p
}
