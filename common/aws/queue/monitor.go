package queue

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/service/sqs"
)

type Monitor struct {
	queueUrl string
	client   *sqs.Client
}

func NewMonitor(queueUrl string, sqsClient *sqs.Client) *Monitor {
	return &Monitor{queueUrl, sqsClient}
}

func (m Monitor) GetQueueUtilization(ctx context.Context) (int, int, error) {
	return GetQueueUtilization(ctx, m.queueUrl, m.client)
}
