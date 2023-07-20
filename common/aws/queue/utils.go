package queue

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

	"github.com/ceramicnetwork/go-cas"
	"github.com/ceramicnetwork/go-cas/common"
)

type QueueType string

const (
	QueueType_Validate QueueType = "validate"
	QueueType_Ready    QueueType = "ready"
	QueueType_Batch    QueueType = "batch"
	QueueType_Status   QueueType = "status"
	QueueType_Failure  QueueType = "failure"
	QueueType_DLQ      QueueType = "dlq"
)

const defaultVisibilityTimeout = 5 * time.Minute
const DefaultMaxReceiveCount = 3

type QueueRedrivePolicy struct {
	DeadLetterTargetArn string `json:"deadLetterTargetArn"`
	MaxReceiveCount     int    `json:"maxReceiveCount"`
}

func CreateQueue(ctx context.Context, sqsClient *sqs.Client, opts PublisherOpts) (string, error) {
	visibilityTimeout := defaultVisibilityTimeout
	if opts.VisibilityTimeout != nil {
		visibilityTimeout = *opts.VisibilityTimeout
	}
	createQueueIn := sqs.CreateQueueInput{
		QueueName: aws.String(queueName(opts.QueueType)),
		Attributes: map[string]string{
			string(types.QueueAttributeNameVisibilityTimeout): strconv.Itoa(int(visibilityTimeout.Seconds())),
		},
	}
	// Configure redrive policy, if specified.
	if opts.RedrivePolicy != nil && len(opts.RedrivePolicy.DeadLetterTargetArn) > 0 && opts.RedrivePolicy.MaxReceiveCount > 0 {
		marshaledRedrivePolicy, _ := json.Marshal(opts.RedrivePolicy)
		createQueueIn.Attributes[string(types.QueueAttributeNameRedrivePolicy)] = string(marshaledRedrivePolicy)
	}

	httpCtx, httpCancel := context.WithTimeout(ctx, common.DefaultRpcWaitTime)
	defer httpCancel()

	if createQueueOut, err := sqsClient.CreateQueue(httpCtx, &createQueueIn); err != nil {
		return "", err
	} else {
		return *createQueueOut.QueueUrl, nil
	}
}

func GetQueueUtilization(ctx context.Context, queueUrl string, sqsClient *sqs.Client) (int, int, error) {
	if queueAttr, err := getQueueAttributes(ctx, queueUrl, sqsClient); err != nil {
		return 0, 0, err
	} else if numMsgsUnprocessedStr, found := queueAttr[string(types.QueueAttributeNameApproximateNumberOfMessages)]; found {
		if numMsgsUnprocessed, err := strconv.Atoi(numMsgsUnprocessedStr); err != nil {
			return 0, 0, err
		} else if numMsgsInFlightStr, found := queueAttr[string(types.QueueAttributeNameApproximateNumberOfMessagesNotVisible)]; found {
			if numMsgsInFlight, err := strconv.Atoi(numMsgsInFlightStr); err != nil {
				return 0, 0, err
			} else {
				return numMsgsUnprocessed, numMsgsInFlight, nil
			}
		}
	}
	return 0, 0, nil
}

func GetQueueUrl(ctx context.Context, queueType QueueType, sqsClient *sqs.Client) (string, error) {
	getQueueUrlIn := sqs.GetQueueUrlInput{
		QueueName: aws.String(queueName(queueType)),
	}

	httpCtx, httpCancel := context.WithTimeout(ctx, common.DefaultRpcWaitTime)
	defer httpCancel()

	if getQueueUrlOut, err := sqsClient.GetQueueUrl(httpCtx, &getQueueUrlIn); err != nil {
		return "", nil
	} else {
		return *getQueueUrlOut.QueueUrl, nil
	}
}

func GetQueueArn(ctx context.Context, queueUrl string, sqsClient *sqs.Client) (string, error) {
	if queueAttr, err := getQueueAttributes(ctx, queueUrl, sqsClient); err != nil {
		return "", err
	} else {
		return queueAttr[string(types.QueueAttributeNameQueueArn)], nil
	}
}

func getQueueAttributes(ctx context.Context, queueUrl string, sqsClient *sqs.Client) (map[string]string, error) {
	getQueueAttrIn := sqs.GetQueueAttributesInput{
		QueueUrl:       aws.String(queueUrl),
		AttributeNames: []types.QueueAttributeName{types.QueueAttributeNameAll},
	}

	httpCtx, httpCancel := context.WithTimeout(ctx, common.DefaultRpcWaitTime)
	defer httpCancel()

	if getQueueAttrOut, err := sqsClient.GetQueueAttributes(httpCtx, &getQueueAttrIn); err != nil {
		return nil, nil
	} else {
		return getQueueAttrOut.Attributes, nil
	}
}

func queueName(queueType QueueType) string {
	return fmt.Sprintf("cas-anchor-%s-%s", os.Getenv(cas.Env_Env), string(queueType))
}
