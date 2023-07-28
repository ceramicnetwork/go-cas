package queue

import (
	"math"

	"github.com/abevier/go-sqs/gosqs"
	"github.com/ceramicnetwork/go-cas/models"
)

const defaultNumConsumerWorkers = 100

type Consumer struct {
	queueType QueueType
	consumer  *gosqs.SQSConsumer
	logger    models.Logger
}

func NewConsumer(logger models.Logger, publisher *Publisher, callback gosqs.MessageCallbackFunc, numWorkers *int) *Consumer {
	var maxWorkers float64 = defaultNumConsumerWorkers
	if numWorkers != nil {
		// Don't go below the default number of workers
		maxWorkers = math.Max(maxWorkers, float64(*numWorkers))
	}
	maxReceivedMessages := math.Ceil(float64(maxWorkers) * 1.2)
	maxInflightRequests := math.Ceil(maxReceivedMessages / 10)
	qOpts := gosqs.Opts{
		MaxReceivedMessages:               int(maxReceivedMessages),
		MaxWorkers:                        int(maxWorkers),
		MaxInflightReceiveMessageRequests: int(maxInflightRequests),
	}
	return &Consumer{publisher.queueType, gosqs.NewConsumer(qOpts, publisher.publisher, callback), logger}
}

func (c Consumer) Start() {
	c.consumer.Start()
	c.logger.Infof("%s: started", c.queueType)
}

func (c Consumer) Shutdown() {
	c.consumer.Shutdown()
	c.logger.Infof("%s: stopped", c.queueType)
}

func (c Consumer) WaitForRxShutdown() {
	c.consumer.WaitForRxShutdown()
	c.logger.Infof("%s: rx stopped", c.queueType)
}
