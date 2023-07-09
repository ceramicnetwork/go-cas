package ddb

import (
	"context"
	"log"
	"os"
	"strconv"
	"time"

	"github.com/google/uuid"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/ceramicnetwork/go-cas/models"
)

type JobDatabase struct {
	ddbClient *dynamodb.Client
	jobTable  string
}

func NewJobDb(ctx context.Context, ddbClient *dynamodb.Client) *JobDatabase {
	jobTable := "ceramic-" + os.Getenv("ENV") + "-ops"
	jdb := JobDatabase{ddbClient, jobTable}
	if err := jdb.createJobTable(ctx); err != nil {
		log.Fatalf("job: table creation failed: %v", err)
	}
	return &jdb
}

func (jdb *JobDatabase) createJobTable(ctx context.Context) error {
	createJobTableInput := dynamodb.CreateTableInput{
		AttributeDefinitions: []types.AttributeDefinition{
			{
				AttributeName: aws.String("stage"),
				AttributeType: "S",
			},
			{
				AttributeName: aws.String("ts"),
				AttributeType: "N",
			},
		},
		KeySchema: []types.KeySchemaElement{
			{
				AttributeName: aws.String("stage"),
				KeyType:       "HASH",
			},
			{
				AttributeName: aws.String("ts"),
				KeyType:       "RANGE",
			},
		},
		TableName: aws.String(jdb.jobTable),
		ProvisionedThroughput: &types.ProvisionedThroughput{
			ReadCapacityUnits:  aws.Int64(1),
			WriteCapacityUnits: aws.Int64(1),
		},
	}
	return createTable(ctx, jdb.ddbClient, &createJobTableInput)
}

func (jdb *JobDatabase) CreateJob(ctx context.Context) error {
	newJob := map[string]interface{}{
		models.JobParam_Id:    uuid.New().String(),
		models.JobParam_Ts:    time.Now(),
		models.JobParam_Stage: models.DefaultJobState,
		models.JobParam_Type:  models.JobType_Anchor,
		models.JobParam_Params: map[string]interface{}{
			models.JobParam_Version: models.WorkerVersion, // this will launch a CASv5 Worker
		},
	}
	attributeValues, err := attributevalue.MarshalMapWithOptions(newJob, func(options *attributevalue.EncoderOptions) {
		options.EncodeTime = func(time time.Time) (types.AttributeValue, error) {
			return &types.AttributeValueMemberN{Value: strconv.FormatInt(time.UnixMilli(), 10)}, nil
		}
	})
	if err != nil {
		return err
	} else {
		httpCtx, httpCancel := context.WithTimeout(ctx, models.DefaultHttpWaitTime)
		defer httpCancel()

		_, err = jdb.ddbClient.PutItem(httpCtx, &dynamodb.PutItemInput{
			TableName: aws.String(jdb.jobTable),
			Item:      attributeValues,
		})
		return err
	}
}
