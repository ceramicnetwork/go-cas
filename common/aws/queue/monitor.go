package queue

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/service/sqs"

	"github.com/ceramicnetwork/go-cas/models"
)

type Monitor struct {
	queueUrl string
	client   *sqs.Client
}

func NewMonitor(queueUrl string, sqsClient *sqs.Client) models.QueueMonitor {
	return &Monitor{queueUrl, sqsClient}
}

func (m Monitor) GetUtilization(ctx context.Context) (int, int, error) {
	return GetQueueUtilization(ctx, m.queueUrl, m.client)
}
