package utils

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"

	"github.com/ceramicnetwork/go-cas/models"
)

func AwsConfigWithOverride(customEndpoint string) (aws.Config, error) {
	ctx, cancel := context.WithTimeout(context.Background(), models.DefaultHttpWaitTime)
	defer cancel()

	endpointResolver := aws.EndpointResolverWithOptionsFunc(func(service, region string, options ...interface{}) (aws.Endpoint, error) {
		return aws.Endpoint{
			PartitionID:   "aws",
			URL:           customEndpoint,
			SigningRegion: os.Getenv("AWS_REGION"),
		}, nil
	})
	return config.LoadDefaultConfig(ctx, config.WithEndpointResolverWithOptions(endpointResolver))
}

func AwsConfig() (aws.Config, error) {
	awsEndpoint := os.Getenv("AWS_ENDPOINT")
	if len(awsEndpoint) > 0 {
		log.Printf("config: using custom global aws endpoint: %s", awsEndpoint)
		return AwsConfigWithOverride(awsEndpoint)
	}
	// Load the default configuration
	ctx, cancel := context.WithTimeout(context.Background(), models.DefaultHttpWaitTime)
	defer cancel()

	return config.LoadDefaultConfig(ctx, config.WithRegion(os.Getenv("AWS_REGION")))
}

func CreateQueue(queueType models.QueueType, sqsClient *sqs.Client, redrivePolicy *models.QueueRedrivePolicy) (string, error) {
	visibilityTimeout := models.QueueDefaultVisibilityTimeout
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

func GetQueueUrl(queueType models.QueueType, sqsClient *sqs.Client) (string, error) {
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

func queueName(queueType models.QueueType) string {
	return fmt.Sprintf("cas-anchor-%s-%s", os.Getenv("ENV"), string(queueType))
}
