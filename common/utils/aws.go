package utils

import (
	"context"
	"log"
	"os"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/smrz2001/go-cas/models"
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
