package queue

import (
	"log"

	"github.com/abevier/go-sqs/gosqs"
)

const consumerMaxWorkers = 100
const consumerMaxReceiveMessageRequests = 12

type Consumer struct {
	queueType QueueType
	consumer  *gosqs.SQSConsumer
}

func NewConsumer(publisher *Publisher, callback gosqs.MessageCallbackFunc) *Consumer {
	qOpts := gosqs.Opts{
		MaxReceivedMessages:               consumerMaxWorkers * 1.2,
		MaxWorkers:                        consumerMaxWorkers,
		MaxInflightReceiveMessageRequests: consumerMaxReceiveMessageRequests,
	}
	return &Consumer{publisher.queueType, gosqs.NewConsumer(qOpts, publisher.publisher, callback)}
}

func (c Consumer) Start() {
	log.Printf("%s: consumer starting", c.queueType)
	c.consumer.Start()
	log.Printf("%s: consumer started", c.queueType)
}

func (c Consumer) Shutdown() {
	log.Printf("%s: consumer stopping", c.queueType)
	c.consumer.Shutdown()
	log.Printf("%s: consumer stopped", c.queueType)
}
