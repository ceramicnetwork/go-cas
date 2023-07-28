package ddb

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"github.com/ceramicnetwork/go-cas"
	"github.com/ceramicnetwork/go-cas/common"
	"github.com/ceramicnetwork/go-cas/models"
)

const idTsIndex = "id-ts-index"

type JobDatabase struct {
	ddbClient *dynamodb.Client
	jobTable  string
	logger    models.Logger
}

func NewJobDb(ctx context.Context, logger models.Logger, ddbClient *dynamodb.Client) *JobDatabase {
	jobTable := "ceramic-" + os.Getenv(cas.Env_Env) + "-ops"
	jdb := JobDatabase{ddbClient, jobTable, logger}
	if err := jdb.createJobTable(ctx); err != nil {
		jdb.logger.Fatalf("table creation failed: %v", err)
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
	return createTable(ctx, jdb.logger, jdb.ddbClient, &createJobTableInput)
}

func (jdb *JobDatabase) CreateJob(ctx context.Context) (string, error) {
	jobParams := map[string]interface{}{
		models.JobParam_Version: models.WorkerVersion, // this will launch a CASv5 Worker
	}
	// If an override anchor contract address is available, pass it through to the job.
	if contractAddress, found := os.LookupEnv(models.Env_AnchorContractAddress); found {
		jobParams[models.JobParam_Overrides] = map[string]string{
			models.AnchorOverrides_ContractAddress: contractAddress,
		}
	}
	newJob := models.NewJob(models.JobType_Anchor, jobParams)
	attributeValues, err := attributevalue.MarshalMapWithOptions(newJob, func(options *attributevalue.EncoderOptions) {
		options.EncodeTime = func(time time.Time) (types.AttributeValue, error) {
			return &types.AttributeValueMemberN{Value: strconv.FormatInt(time.UnixMilli(), 10)}, nil
		}
	})
	if err != nil {
		return "", err
	} else {
		httpCtx, httpCancel := context.WithTimeout(ctx, common.DefaultRpcWaitTime)
		defer httpCancel()

		_, err = jdb.ddbClient.PutItem(httpCtx, &dynamodb.PutItemInput{
			TableName: aws.String(jdb.jobTable),
			Item:      attributeValues,
		})
		if err != nil {
			return "", err
		} else {
			return newJob.Id, nil
		}
	}
}

func (jdb *JobDatabase) QueryJob(ctx context.Context, id string) (*models.JobState, error) {
	queryInput := dynamodb.QueryInput{
		TableName:                 aws.String(jdb.jobTable),
		IndexName:                 aws.String(idTsIndex),
		KeyConditionExpression:    aws.String("#id = :id"),
		ExpressionAttributeNames:  map[string]string{"#id": "id"},
		ExpressionAttributeValues: map[string]types.AttributeValue{":id": &types.AttributeValueMemberS{Value: id}},
		ScanIndexForward:          aws.Bool(false), // descending order so we get the latest job state
	}

	httpCtx, httpCancel := context.WithTimeout(ctx, common.DefaultRpcWaitTime)
	defer httpCancel()

	if queryOutput, err := jdb.ddbClient.Query(httpCtx, &queryInput); err != nil {
		return nil, err
	} else if queryOutput.Count > 0 {
		job := new(models.JobState)
		if err = attributevalue.UnmarshalMapWithOptions(queryOutput.Items[0], job, func(options *attributevalue.DecoderOptions) {
			options.DecodeTime = attributevalue.DecodeTimeAttributes{
				S: tsDecode,
				N: tsDecode,
			}
		}); err != nil {
			return nil, err
		} else {
			return job, nil
		}
	} else {
		// A job specifically requested must be present
		return nil, fmt.Errorf("job %s not found", id)
	}
}
