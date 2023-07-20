package queue

import (
	"context"
	"encoding/json"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/sqs"

	"github.com/abevier/go-sqs/gosqs"
)

const maxLinger = 250 * time.Millisecond

type PublisherOpts struct {
	QueueType         QueueType
	VisibilityTimeout *time.Duration
	RedrivePolicy     *QueueRedrivePolicy
}

type Publisher struct {
	queueType QueueType
	queueUrl  string
	publisher *gosqs.SQSPublisher
}

func NewPublisher(ctx context.Context, sqsClient *sqs.Client, opts PublisherOpts) (*Publisher, error) {
	// Create the queue if it didn't already exist
	if queueUrl, err := CreateQueue(ctx, sqsClient, opts); err != nil {
		return nil, err
	} else {
		return &Publisher{
			opts.QueueType,
			queueUrl,
			gosqs.NewPublisher(
				sqsClient,
				queueUrl,
				maxLinger,
			),
		}, nil
	}
}

func (p Publisher) GetName() string {
	return string(p.queueType)
}

func (p Publisher) GetUrl() string {
	return p.queueUrl
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
