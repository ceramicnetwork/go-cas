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

	"github.com/3box/pipeline-tools/cd/manager/common/aws/utils"
	"github.com/3box/pipeline-tools/cd/manager/common/job"

	"github.com/ceramicnetwork/go-cas"
	"github.com/ceramicnetwork/go-cas/common"
	"github.com/ceramicnetwork/go-cas/models"
)

type JobDatabase struct {
	ddbClient *dynamodb.Client
	table     string
	logger    models.Logger
}

func NewJobDb(ctx context.Context, logger models.Logger, ddbClient *dynamodb.Client) *JobDatabase {
	jobTable := "ceramic-" + os.Getenv(cas.Env_Env) + "-ops"
	jdb := JobDatabase{ddbClient, jobTable, logger}
	if err := jdb.createJobTable(ctx); err != nil {
		jdb.logger.Fatalf("error creating table: %v", err)
	}
	return &jdb
}

func (jdb *JobDatabase) createJobTable(ctx context.Context) error {
	return job.CreateJobTable(ctx, jdb.ddbClient, jdb.table)
}

func (jdb *JobDatabase) CreateJob(ctx context.Context) (string, error) {
	jobParams := map[string]interface{}{
		job.JobParam_Version: models.WorkerVersion, // this will launch a CASv5 Worker
		job.JobParam_Overrides: map[string]string{
			models.AnchorOverrides_AppMode:                models.AnchorAppMode_ContinualAnchoring,
			models.AnchorOverrides_SchedulerStopAfterNoOp: "true",
		},
	}
	// If an override anchor contract address is available, pass it through to the job.
	if contractAddress, found := os.LookupEnv(models.Env_AnchorContractAddress); found {
		jobParams[job.JobParam_Overrides].(map[string]string)[models.AnchorOverrides_ContractAddress] = contractAddress
	}
	newJob := models.NewJob(job.JobType_Anchor, jobParams)
	attributeValues, err := attributevalue.MarshalMapWithOptions(newJob, func(options *attributevalue.EncoderOptions) {
		options.EncodeTime = func(time time.Time) (types.AttributeValue, error) {
			return &types.AttributeValueMemberN{Value: strconv.FormatInt(time.UnixNano(), 10)}, nil
		}
	})
	if err != nil {
		return "", err
	} else {
		httpCtx, httpCancel := context.WithTimeout(ctx, common.DefaultRpcWaitTime)
		defer httpCancel()

		_, err = jdb.ddbClient.PutItem(httpCtx, &dynamodb.PutItemInput{
			TableName: aws.String(jdb.table),
			Item:      attributeValues,
		})
		if err != nil {
			return "", err
		} else {
			return newJob.Job, nil
		}
	}
}

func (jdb *JobDatabase) QueryJob(ctx context.Context, jobId string) (*job.JobState, error) {
	queryInput := dynamodb.QueryInput{
		TableName:                 aws.String(jdb.table),
		IndexName:                 aws.String(job.JobTsIndex),
		KeyConditionExpression:    aws.String("#job = :job"),
		ExpressionAttributeNames:  map[string]string{"#job": "job"},
		ExpressionAttributeValues: map[string]types.AttributeValue{":job": &types.AttributeValueMemberS{Value: jobId}},
		ScanIndexForward:          aws.Bool(false), // descending order so we get the latest job state
	}

	httpCtx, httpCancel := context.WithTimeout(ctx, common.DefaultRpcWaitTime)
	defer httpCancel()

	if queryOutput, err := jdb.ddbClient.Query(httpCtx, &queryInput); err != nil {
		return nil, err
	} else if queryOutput.Count > 0 {
		j := new(job.JobState)
		if err = attributevalue.UnmarshalMapWithOptions(queryOutput.Items[0], j, func(options *attributevalue.DecoderOptions) {
			options.DecodeTime = attributevalue.DecodeTimeAttributes{
				S: utils.TsDecode,
				N: utils.TsDecode,
			}
		}); err != nil {
			return nil, err
		} else {
			return j, nil
		}
	} else {
		// A job specifically requested must be present
		return nil, fmt.Errorf("job %s not found", jobId)
	}
}
