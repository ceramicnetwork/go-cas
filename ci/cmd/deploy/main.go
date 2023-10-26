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

	"github.com/3box/pipeline-tools/cd/manager/common/job"

	"github.com/ceramicnetwork/go-cas"
	"github.com/ceramicnetwork/go-cas/common/aws/config"
	"github.com/ceramicnetwork/go-cas/models"
)

func main() {
	ctx := context.Background()
	if id, err := createJob(ctx); err != nil {
		log.Fatalf("deploy: error creating deployment job: %v", err)
	} else {
		log.Printf("deploy: created job with id = %s", id)
	}
}

func createJob(ctx context.Context) (string, error) {
	newJob := models.NewJob(job.JobType_Deploy, map[string]interface{}{
		"component": models.DeployComponent,
		"sha":       "latest",
		"shaTag":    os.Getenv(cas.Env_ShaTag),
	})
	// Marshal the CD Manager job into DynamoDB-JSON, which is different from regular JSON and requires data types to be
	// specified explicitly. The time encode function ensures that the timestamp has millisecond-resolution, which is
	// what the CD Manager expects.
	if attributeValues, err := attributevalue.MarshalMapWithOptions(newJob, func(options *attributevalue.EncoderOptions) {
		options.EncodeTime = func(time time.Time) (types.AttributeValue, error) {
			return &types.AttributeValueMemberN{Value: strconv.FormatInt(time.UnixNano(), 10)}, nil
		}
	}); err != nil {
		return "", err
	} else if awsCfg, err := config.AwsConfig(ctx); err != nil {
		return "", err
	} else {
		envTag := os.Getenv(cas.Env_EnvTag)
		client := dynamodb.NewFromConfig(awsCfg)
		if _, err = client.PutItem(ctx, &dynamodb.PutItemInput{
			TableName: aws.String(fmt.Sprintf("ceramic-%s-ops", envTag)),
			Item:      attributeValues,
		}); err != nil {
			return "", err
		} else if envTag == cas.EnvTag_Dev {
			// Also deploy to QA when deploying to Dev
			if _, err = client.PutItem(ctx, &dynamodb.PutItemInput{
				TableName: aws.String(fmt.Sprintf("ceramic-%s-ops", cas.EnvTag_Qa)),
				Item:      attributeValues,
			}); err != nil {
				return "", err
			}
		}
		return newJob.Job, nil
	}
}
