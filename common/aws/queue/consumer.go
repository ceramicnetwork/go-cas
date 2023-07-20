package queue

import (
	"log"
	"math"

	"github.com/abevier/go-sqs/gosqs"
)

const defaultNumConsumerWorkers = 100

type Consumer struct {
	queueType QueueType
	consumer  *gosqs.SQSConsumer
}

func NewConsumer(publisher *Publisher, callback gosqs.MessageCallbackFunc, numWorkers *int) *Consumer {
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
	return &Consumer{publisher.queueType, gosqs.NewConsumer(qOpts, publisher.publisher, callback)}
}

func (c Consumer) Start() {
	log.Printf("%s: consumer starting...", c.queueType)
	c.consumer.Start()
	log.Printf("%s: consumer started", c.queueType)
}

func (c Consumer) Shutdown() {
	log.Printf("%s: consumer stopping...", c.queueType)
	c.consumer.Shutdown()
	log.Printf("%s: consumer stopped", c.queueType)
}

func (c Consumer) WaitForRxShutdown() {
	log.Printf("%s: consumer rx stopping...", c.queueType)
	c.consumer.WaitForRxShutdown()
	log.Printf("%s: consumer rx stopped", c.queueType)
}
