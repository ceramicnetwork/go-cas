package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"strconv"
	"time"

	"github.com/google/uuid"

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

func main() {
	ctx := context.Background()
	if err := createJob(ctx); err != nil {
		log.Fatalf("deploy: error creating deployment job: %v", err)
	}
}

func createJob(ctx context.Context) error {
	newJob := map[string]interface{}{
		"id":    uuid.New().String(),
		"ts":    time.Now(),
		"stage": models.DefaultJobState,
		"type":  models.JobType_Deploy,
		"params": map[string]string{
			"component": models.DeployComponent,
			"sha":       "latest",
			"shaTag":    os.Getenv("SHA_TAG"),
		},
	}
	// Marshal the CD Manager job into DynamoDB-JSON, which is different from regular JSON and requires data types to be
	// specified explicitly. The time encode function ensures that the timestamp has millisecond-resolution, which is
	// what the CD Manager expects.
	attributeValues, err := attributevalue.MarshalMapWithOptions(newJob, func(options *attributevalue.EncoderOptions) {
		options.EncodeTime = func(time time.Time) (types.AttributeValue, error) {
			return &types.AttributeValueMemberN{Value: strconv.FormatInt(time.UnixMilli(), 10)}, nil
		}
	})
	if err != nil {
		return err
	} else {
		awsCfg, err := config.AwsConfig()
		if err != nil {
			return err
		}
		_, err = dynamodb.NewFromConfig(awsCfg).PutItem(ctx, &dynamodb.PutItemInput{
			TableName: aws.String(fmt.Sprintf("ceramic-%s-ops", os.Getenv(Env_EnvTag))),
			Item:      attributeValues,
		})
		return err
	}
}
