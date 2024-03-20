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

type Type string

const (
	Type_Validate Type = "validate"
	Type_Ready    Type = "ready"
	Type_Batch    Type = "batch"
	Type_Status   Type = "status"
	Type_Failure  Type = "failure"
	Type_DLQ      Type = "dlq"
	Type_IPFS     Type = "ipfs"
)

const defaultVisibilityTimeout = 5 * time.Minute
const DefaultMaxReceiveCount = 3

type redrivePolicy struct {
	DeadLetterTargetArn string `json:"deadLetterTargetArn"`
	MaxReceiveCount     int    `json:"maxReceiveCount"`
}

func CreateQueue(ctx context.Context, sqsClient *sqs.Client, opts Opts, index int) (string, string, string, error) {
	visibilityTimeout := defaultVisibilityTimeout
	if opts.VisibilityTimeout != nil {
		visibilityTimeout = *opts.VisibilityTimeout
	}
	// Append a non-zero index to the queue name if specified
	name := queueName(opts.QueueType)
	if index > 0 {
		name = fmt.Sprintf("%s-%d", name, index)
	}
	createQueueIn := sqs.CreateQueueInput{
		QueueName: aws.String(name),
		Attributes: map[string]string{
			string(types.QueueAttributeNameVisibilityTimeout): strconv.Itoa(int(visibilityTimeout.Seconds())),
		},
	}
	// Configure redrive policy, if specified.
	if opts.RedriveOpts != nil && len(opts.RedriveOpts.DlqId) > 0 && opts.RedriveOpts.MaxReceiveCount > 0 {
		marshaledRedrivePolicy, _ := json.Marshal(redrivePolicy{
			DeadLetterTargetArn: opts.RedriveOpts.DlqId,
			MaxReceiveCount:     opts.RedriveOpts.MaxReceiveCount,
		})
		createQueueIn.Attributes[string(types.QueueAttributeNameRedrivePolicy)] = string(marshaledRedrivePolicy)
	}

	httpCtx, httpCancel := context.WithTimeout(ctx, common.DefaultRpcWaitTime)
	defer httpCancel()

	if createQueueOut, err := sqsClient.CreateQueue(httpCtx, &createQueueIn); err != nil {
		return "", "", "", err
	} else if arn, err := getQueueArn(ctx, *createQueueOut.QueueUrl, sqsClient); err != nil {
		return "", "", "", err
	} else {
		return *createQueueOut.QueueUrl, arn, name, nil
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

func getQueueArn(ctx context.Context, queueUrl string, sqsClient *sqs.Client) (string, error) {
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

func queueName(queueType Type) string {
	return fmt.Sprintf("cas-anchor-%s-%s", os.Getenv(cas.Env_Env), string(queueType))
}
