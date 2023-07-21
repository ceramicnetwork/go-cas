package queue

import (
	"math"

	"github.com/abevier/go-sqs/gosqs"
	"github.com/ceramicnetwork/go-cas/models"
)

const defaultConsumerMaxWorkers = 100

type ConsumerOpts struct {
	MaxReceivedMessages int
	MaxWorkers          int
	MaxInflightRequests int
}

type Consumer struct {
	queueType QueueType
	consumer  *gosqs.SQSConsumer
	logger    models.Logger
}

func NewConsumer(logger models.Logger, publisher *Publisher, callback gosqs.MessageCallbackFunc, opts *ConsumerOpts) *Consumer {
	maxWorkers := defaultConsumerMaxWorkers
	maxReceivedMessages := math.Ceil(float64(maxWorkers) * 1.2)
	maxInflightRequests := math.Ceil(maxReceivedMessages / 10)
	if opts != nil {
		maxReceivedMessages = float64(opts.MaxReceivedMessages)
		maxWorkers = opts.MaxWorkers
		maxInflightRequests = float64(opts.MaxInflightRequests)
	}
	qOpts := gosqs.Opts{
		MaxReceivedMessages:               int(maxReceivedMessages),
		MaxWorkers:                        maxWorkers,
		MaxInflightReceiveMessageRequests: int(maxInflightRequests),
	}
	return &Consumer{publisher.queueType, gosqs.NewConsumer(qOpts, publisher.publisher, callback), logger}
}

func (c Consumer) Start() {
	c.logger.Infof("%s: consumer starting...", c.queueType)
	c.consumer.Start()
	c.logger.Infof("%s: consumer started", c.queueType)
}

func (c Consumer) Shutdown() {
	c.logger.Infof("%s: consumer stopping...", c.queueType)
	c.consumer.Shutdown()
	c.logger.Infof("%s: consumer stopped", c.queueType)
}

func (c Consumer) WaitForRxShutdown() {
	c.logger.Infof("%s: consumer rx stopping...", c.queueType)
	c.consumer.WaitForRxShutdown()
	c.logger.Infof("%s: consumer rx stopped", c.queueType)
}
