package queue

import (
	"github.com/abevier/go-sqs/gosqs"

	"github.com/smrz2001/go-cas/models"
)

type Consumer struct {
	consumer *gosqs.SQSConsumer
}

func NewConsumer(publisher *Publisher, callback gosqs.MessageCallbackFunc) *Consumer {
	qOpts := gosqs.Opts{
		MaxReceivedMessages:               models.QueueMaxWorkers * 1.2,
		MaxWorkers:                        models.QueueMaxWorkers,
		MaxInflightReceiveMessageRequests: models.QueueMaxReceiveMessageRequests,
	}
	return &Consumer{gosqs.NewConsumer(qOpts, publisher.publisher, callback)}
}

func (c Consumer) Start() {
	c.consumer.Start()
}
