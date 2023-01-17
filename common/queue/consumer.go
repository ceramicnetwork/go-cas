package queue

import (
	"github.com/aws/aws-sdk-go-v2/service/sqs"

	"github.com/abevier/go-sqs/gosqs"

	"github.com/smrz2001/go-cas/common/utils"
	"github.com/smrz2001/go-cas/models"
)

type Consumer struct {
	consumer *gosqs.SQSConsumer
}

func NewConsumer(queueType models.QueueType, sqsClient *sqs.Client, callback gosqs.MessageCallbackFunc) *Consumer {
	qOpts := gosqs.Opts{
		MaxReceivedMessages:               models.DefaultMaxReceivedMessages,
		MaxWorkers:                        models.DefaultMaxNumWorkers,
		MaxInflightReceiveMessageRequests: models.DefaultMaxInflightMessages,
	}
	publisher := gosqs.NewPublisher(
		sqsClient,
		utils.GetQueueUrl(sqsClient, queueType),
		models.DefaultBatchMaxLinger,
	)
	return &Consumer{gosqs.NewConsumer(qOpts, publisher, callback)}
}

func (c Consumer) Start() {
	c.consumer.Start()
}
