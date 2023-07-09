package queue

import (
	"context"
	"encoding/json"

	"github.com/aws/aws-sdk-go-v2/service/sqs"

	"github.com/abevier/go-sqs/gosqs"
)

type Publisher struct {
	queueType QueueType
	QueueUrl  string
	publisher *gosqs.SQSPublisher
}

func NewPublisher(ctx context.Context, queueType QueueType, sqsClient *sqs.Client, redrivePolicy *QueueRedrivePolicy) (*Publisher, error) {
	// Create the queue if it didn't already exist
	if queueUrl, err := CreateQueue(ctx, queueType, sqsClient, redrivePolicy); err != nil {
		return nil, err
	} else {
		return &Publisher{
			queueType,
			queueUrl,
			gosqs.NewPublisher(
				sqsClient,
				queueUrl,
				QueueMaxLinger,
			)}, nil
	}
}

func (p Publisher) SendMessage(ctx context.Context, event any) (string, error) {
	if eventBody, err := json.Marshal(event); err != nil {
		return "", err
	} else if msgId, err := p.publisher.SendMessage(ctx, string(eventBody)); err != nil {
		return "", err
	} else {
		return msgId, nil
	}
}
