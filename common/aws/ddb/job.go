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
	client   *dynamodb.Client
	jobTable string
}

func NewJobDb(cfg aws.Config) *JobDatabase {
	jobTable := "ceramic-" + os.Getenv("ENV") + "-ops"
	client := dynamodb.NewFromConfig(cfg)
	jdb := JobDatabase{
		client,
		jobTable,
	}
	if err := jdb.createJobTable(); err != nil {
		log.Fatalf("job: table creation failed: %v", err)
	}
	return &jdb
}

func (jdb *JobDatabase) createJobTable() error {
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
	return createTable(jdb.client, &createJobTableInput)
}

func (jdb *JobDatabase) CreateJob() error {
	newJob := map[string]interface{}{
		models.JobParam_Id:    uuid.New().String(),
		models.JobParam_Ts:    time.Now(),
		models.JobParam_Stage: models.DefaultJobState,
		models.JobParam_Type:  models.JobType_Anchor,
		models.JobParam_Params: map[string]string{
			models.JobParams_Version:  models.WorkerVersion, // this will launch a CASv5 Worker
			models.JobParams_Contract: os.Getenv("ANCHOR_CONTRACT_ADDRESS"),
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
		ctx, cancel := context.WithTimeout(context.Background(), models.DefaultHttpWaitTime)
		defer cancel()

		_, err = jdb.client.PutItem(ctx, &dynamodb.PutItemInput{
			TableName: aws.String(jdb.jobTable),
			Item:      attributeValues,
		})
		return err
	}
}
