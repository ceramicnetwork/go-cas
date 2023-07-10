package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"github.com/ceramicnetwork/go-cas/common/aws/config"
	"github.com/ceramicnetwork/go-cas/models"
)

const (
	Env_EnvTag = "ENV_TAG"
)

const (
	EnvTag_Dev  = "dev"
	EnvTag_Qa   = "qa"
	EnvTag_Tnet = "tnet"
	EnvTag_Prod = "prod"
)

func main() {
	ctx := context.Background()
	if err := createJob(ctx); err != nil {
		log.Fatalf("deploy: error creating deployment job: %v", err)
	}
}

func createJob(ctx context.Context) error {
	newJob := models.NewJob(models.JobType_Deploy, map[string]interface{}{
		"component": models.DeployComponent,
		"sha":       "latest",
		"shaTag":    os.Getenv("SHA_TAG"),
	})
	// Marshal the CD Manager job into DynamoDB-JSON, which is different from regular JSON and requires data types to be
	// specified explicitly. The time encode function ensures that the timestamp has millisecond-resolution, which is
	// what the CD Manager expects.
	if attributeValues, err := attributevalue.MarshalMapWithOptions(newJob, func(options *attributevalue.EncoderOptions) {
		options.EncodeTime = func(time time.Time) (types.AttributeValue, error) {
			return &types.AttributeValueMemberN{Value: strconv.FormatInt(time.UnixMilli(), 10)}, nil
		}
	}); err != nil {
		return err
	} else if awsCfg, err := config.AwsConfig(ctx); err != nil {
		return err
	} else {
		envTag := os.Getenv(Env_EnvTag)
		client := dynamodb.NewFromConfig(awsCfg)
		if _, err = client.PutItem(ctx, &dynamodb.PutItemInput{
			TableName: aws.String(fmt.Sprintf("ceramic-%s-ops", envTag)),
			Item:      attributeValues,
		}); err != nil {
			return err
		} else if envTag == EnvTag_Dev {
			// Also deploy to QA when deploying to Dev
			_, err = client.PutItem(ctx, &dynamodb.PutItemInput{
				TableName: aws.String(fmt.Sprintf("ceramic-%s-ops", EnvTag_Qa)),
				Item:      attributeValues,
			})
		}
		return err
	}
}
