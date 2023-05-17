package aws

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"

	"github.com/ceramicnetwork/go-cas/models"
)

type QueueType string

const (
	QueueType_Validate QueueType = "validate"
	QueueType_Ready    QueueType = "ready"
	QueueType_Batch    QueueType = "batch"
	QueueType_Failure  QueueType = "failure"
	QueueType_DLQ      QueueType = "dlq"
)

const QueueMaxLinger = 250 * time.Millisecond
const QueueDefaultVisibilityTimeout = 5 * time.Minute
const QueueMaxReceiveCount = 3

type QueueRedrivePolicy struct {
	DeadLetterTargetArn string `json:"deadLetterTargetArn"`
	MaxReceiveCount     int    `json:"maxReceiveCount"`
}

func CreateQueue(queueType QueueType, sqsClient *sqs.Client, redrivePolicy *QueueRedrivePolicy) (string, error) {
	visibilityTimeout := QueueDefaultVisibilityTimeout
	if configVisibilityTimeout, found := os.LookupEnv("QUEUE_VISIBILITY_TIMEOUT"); found {
		if parsedVisibilityTimeout, err := time.ParseDuration(configVisibilityTimeout); err == nil {
			visibilityTimeout = parsedVisibilityTimeout
		}
	}
	createQueueIn := sqs.CreateQueueInput{
		QueueName: aws.String(queueName(queueType)),
		Attributes: map[string]string{
			string(types.QueueAttributeNameVisibilityTimeout): strconv.Itoa(int(visibilityTimeout.Seconds())),
		},
	}
	// Configure redrive policy, if specified.
	if redrivePolicy != nil && len(redrivePolicy.DeadLetterTargetArn) > 0 && redrivePolicy.MaxReceiveCount > 0 {
		marshaledRedrivePolicy, _ := json.Marshal(redrivePolicy)
		createQueueIn.Attributes[string(types.QueueAttributeNameRedrivePolicy)] = string(marshaledRedrivePolicy)
	}
	ctx, cancel := context.WithTimeout(context.Background(), models.DefaultHttpWaitTime)
	defer cancel()

	if createQueueOut, err := sqsClient.CreateQueue(ctx, &createQueueIn); err != nil {
		return "", nil
	} else {
		return *createQueueOut.QueueUrl, nil
	}
}

func GetQueueUrl(queueType QueueType, sqsClient *sqs.Client) (string, error) {
	getQueueUrlIn := sqs.GetQueueUrlInput{
		QueueName: aws.String(queueName(queueType)),
	}
	ctx, cancel := context.WithTimeout(context.Background(), models.DefaultHttpWaitTime)
	defer cancel()

	if getQueueUrlOut, err := sqsClient.GetQueueUrl(ctx, &getQueueUrlIn); err != nil {
		return "", nil
	} else {
		return *getQueueUrlOut.QueueUrl, nil
	}
}

func GetQueueArn(queueUrl string, sqsClient *sqs.Client) (string, error) {
	if queueAttr, err := getQueueAttributes(queueUrl, sqsClient); err != nil {
		return "", err
	} else {
		return queueAttr[string(types.QueueAttributeNameQueueArn)], nil
	}
}

func getQueueAttributes(queueUrl string, sqsClient *sqs.Client) (map[string]string, error) {
	getQueueAttrIn := sqs.GetQueueAttributesInput{
		QueueUrl:       aws.String(queueUrl),
		AttributeNames: []types.QueueAttributeName{types.QueueAttributeNameAll},
	}
	ctx, cancel := context.WithTimeout(context.Background(), models.DefaultHttpWaitTime)
	defer cancel()

	if getQueueAttrOut, err := sqsClient.GetQueueAttributes(ctx, &getQueueAttrIn); err != nil {
		return nil, nil
	} else {
		return getQueueAttrOut.Attributes, nil
	}
}

func queueName(queueType QueueType) string {
	return fmt.Sprintf("cas-anchor-%s-%s", os.Getenv("ENV"), string(queueType))
}
