package queue

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"

	"github.com/abevier/go-sqs/gosqs"

	"github.com/smrz2001/go-cas/models"
)

const publisherMaxLinger = 250 * time.Millisecond

type Publisher struct {
	publisher *gosqs.SQSPublisher
}

func NewPublisher(queueType models.QueueType, sqsClient *sqs.Client) (*Publisher, error) {
	// Create the queue if it didn't already exist
	if queueUrl, err := createQueue(queueType, sqsClient); err != nil {
		return nil, err
	} else {
		return &Publisher{gosqs.NewPublisher(
			sqsClient,
			queueUrl,
			publisherMaxLinger,
		)}, nil
	}
}

func createQueue(queueType models.QueueType, sqsClient *sqs.Client) (string, error) {
	createQueueIn := sqs.CreateQueueInput{
		QueueName: aws.String(fmt.Sprintf("cas-anchor-%s-%s", os.Getenv("ENV"), string(queueType))),
	}

	ctx, cancel := context.WithTimeout(context.Background(), models.DefaultHttpWaitTime)
	defer cancel()

	if createQueueOut, err := sqsClient.CreateQueue(ctx, &createQueueIn); err != nil {
		return "", nil
	} else {
		return *createQueueOut.QueueUrl, nil
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
