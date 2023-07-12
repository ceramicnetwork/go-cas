package queue

import (
	"log"
	"math"

	"github.com/abevier/go-sqs/gosqs"
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
}

func NewConsumer(publisher *Publisher, callback gosqs.MessageCallbackFunc, opts *ConsumerOpts) *Consumer {
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
