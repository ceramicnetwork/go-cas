package queue

import (
	"context"
	"encoding/json"

	"github.com/aws/aws-sdk-go-v2/service/sqs"

	"github.com/abevier/go-sqs/gosqs"

	"github.com/ceramicnetwork/go-cas/common/utils"
	"github.com/ceramicnetwork/go-cas/models"
)

type Publisher struct {
	QueueUrl  string
	publisher *gosqs.SQSPublisher
}

func NewPublisher(queueType models.QueueType, sqsClient *sqs.Client, redrivePolicy *models.QueueRedrivePolicy) (*Publisher, error) {
	// Create the queue if it didn't already exist
	if queueUrl, err := utils.CreateQueue(queueType, sqsClient, redrivePolicy); err != nil {
		return nil, err
	} else {
		return &Publisher{
			queueUrl,
			gosqs.NewPublisher(
				sqsClient,
				queueUrl,
				models.QueueMaxLinger,
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
