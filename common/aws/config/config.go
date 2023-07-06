package config

import (
	"context"
	"log"
	"os"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"

	"github.com/ceramicnetwork/go-cas/models"
)

func AwsConfigWithOverride(ctx context.Context, customEndpoint string) (aws.Config, error) {
	endpointResolver := aws.EndpointResolverWithOptionsFunc(func(service, region string, options ...interface{}) (aws.Endpoint, error) {
		return aws.Endpoint{
			PartitionID:   "aws",
			URL:           customEndpoint,
			SigningRegion: os.Getenv("AWS_REGION"),
		}, nil
	})

	httpCtx, httpCancel := context.WithTimeout(ctx, models.DefaultHttpWaitTime)
	defer httpCancel()

	return config.LoadDefaultConfig(httpCtx, config.WithEndpointResolverWithOptions(endpointResolver))
}

func AwsConfig(ctx context.Context) (aws.Config, error) {
	awsEndpoint := os.Getenv("AWS_ENDPOINT")
	if len(awsEndpoint) > 0 {
		log.Printf("config: using custom global aws endpoint: %s", awsEndpoint)
		return AwsConfigWithOverride(ctx, awsEndpoint)
	}

	httpCtx, httpCancel := context.WithTimeout(ctx, models.DefaultHttpWaitTime)
	defer httpCancel()

	// Load the default configuration
	return config.LoadDefaultConfig(httpCtx, config.WithRegion(os.Getenv("AWS_REGION")))
}
