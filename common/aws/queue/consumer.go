package queue

import "github.com/abevier/go-sqs/gosqs"

const consumerMaxWorkers = 100
const consumerMaxReceiveMessageRequests = 12

type Consumer struct {
	consumer *gosqs.SQSConsumer
}

func NewConsumer(publisher *Publisher, callback gosqs.MessageCallbackFunc) *Consumer {
	qOpts := gosqs.Opts{
		MaxReceivedMessages:               consumerMaxWorkers * 1.2,
		MaxWorkers:                        consumerMaxWorkers,
		MaxInflightReceiveMessageRequests: consumerMaxReceiveMessageRequests,
	}
	return &Consumer{gosqs.NewConsumer(qOpts, publisher.publisher, callback)}
}

func (c Consumer) Start() {
	c.consumer.Start()
}
