package queue

import (
	"context"
	"encoding/json"

	"github.com/aws/aws-sdk-go-v2/service/sqs"

	"github.com/abevier/go-sqs/gosqs"

	"github.com/smrz2001/go-cas/common/utils"
	"github.com/smrz2001/go-cas/models"
)

type Publisher struct {
	publisher *gosqs.SQSPublisher
}

func NewPublisher(queueType models.QueueType, sqsClient *sqs.Client) *Publisher {
	return &Publisher{gosqs.NewPublisher(
		sqsClient,
		utils.GetQueueUrl(sqsClient, queueType),
		models.QueuePublisherMaxLinger,
	)}
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
