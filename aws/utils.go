package cas

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"

	"github.com/abevier/go-sqs/gosqs"

	"github.com/smrz2001/go-cas/models"
)

func GetQueueUrl(client *sqs.Client, qType models.QueueType) string {
	ctx, cancel := context.WithTimeout(context.Background(), models.DefaultHttpWaitTime)
	defer cancel()

	in := sqs.GetQueueUrlInput{
		QueueName:              aws.String(fmt.Sprintf("cas-anchor-%s-%s.fifo", os.Getenv("ENV"), string(qType))),
		QueueOwnerAWSAccountId: aws.String(os.Getenv("ACCOUNT_ID")),
	}
	out, err := client.GetQueueUrl(ctx, &in)
	if err != nil {
		log.Fatalf("newQueue: failed to retrieve %s queue url: %v", string(qType), err)
	}
	return *out.QueueUrl
}

func PublishEvent(ctx context.Context, publisher *gosqs.SQSPublisher, event any) (string, error) {
	if eventBody, err := json.Marshal(event); err != nil {
		return "", err
	} else if msgId, err := publisher.SendMessage(ctx, string(eventBody)); err != nil {
		return "", err
	} else {
		return msgId, nil
	}
}
