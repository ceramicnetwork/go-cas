package ddb

import (
	"context"
	"errors"
	"os"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"github.com/3box/pipeline-tools/cd/manager/common/aws/utils"

	"github.com/ceramicnetwork/go-cas"
	"github.com/ceramicnetwork/go-cas/common"
	"github.com/ceramicnetwork/go-cas/models"
)

var _ models.StateRepository = &StateDatabase{}

type StateDatabase struct {
	client          *dynamodb.Client
	checkpointTable string
	streamTable     string
	tipTable        string
	logger          models.Logger
}

func NewStateDb(ctx context.Context, logger models.Logger, client *dynamodb.Client) *StateDatabase {
	env := os.Getenv(cas.Env_Env)

	tablePfx := "cas-anchor-" + env + "-"
	checkpointTable := tablePfx + "checkpoint"
	streamTable := tablePfx + "stream"
	tipTable := tablePfx + "tip"

	sdb := StateDatabase{
		client,
		checkpointTable,
		streamTable,
		tipTable,
		logger,
	}
	if err := sdb.createCheckpointTable(ctx); err != nil {
		sdb.logger.Fatalf("error creating checkpoint table: %v", err)
	} else if err = sdb.createStreamTable(ctx); err != nil {
		sdb.logger.Fatalf("error creating stream table: %v", err)
	} else if err = sdb.createTipTable(ctx); err != nil {
		sdb.logger.Fatalf("error creating tip table: %v", err)
	}
	return &sdb
}

func (sdb *StateDatabase) createCheckpointTable(ctx context.Context) error {
	createStreamTableInput := dynamodb.CreateTableInput{
		AttributeDefinitions: []types.AttributeDefinition{
			{
				AttributeName: aws.String("name"),
				AttributeType: "S",
			},
		},
		KeySchema: []types.KeySchemaElement{
			{
				AttributeName: aws.String("name"),
				KeyType:       "HASH",
			},
		},
		TableName:   aws.String(sdb.checkpointTable),
		BillingMode: types.BillingModePayPerRequest,
	}
	return utils.CreateTable(ctx, sdb.client, &createStreamTableInput)
}

func (sdb *StateDatabase) createStreamTable(ctx context.Context) error {
	createTableInput := dynamodb.CreateTableInput{
		AttributeDefinitions: []types.AttributeDefinition{
			{
				AttributeName: aws.String("id"),
				AttributeType: "S",
			},
			{
				AttributeName: aws.String("cid"),
				AttributeType: "S",
			},
		},
		KeySchema: []types.KeySchemaElement{
			{
				AttributeName: aws.String("id"),
				KeyType:       "HASH",
			},
			{
				AttributeName: aws.String("cid"),
				KeyType:       "RANGE",
			},
		},
		TableName:   aws.String(sdb.streamTable),
		BillingMode: types.BillingModePayPerRequest,
	}
	return utils.CreateTable(ctx, sdb.client, &createTableInput)
}

func (sdb *StateDatabase) createTipTable(ctx context.Context) error {
	createTableInput := dynamodb.CreateTableInput{
		AttributeDefinitions: []types.AttributeDefinition{
			{
				AttributeName: aws.String("id"),
				AttributeType: "S",
			},
			{
				AttributeName: aws.String("org"),
				AttributeType: "S",
			},
		},
		KeySchema: []types.KeySchemaElement{
			{
				AttributeName: aws.String("id"),
				KeyType:       "HASH",
			},
			{
				AttributeName: aws.String("org"),
				KeyType:       "RANGE",
			},
		},
		TableName:   aws.String(sdb.tipTable),
		BillingMode: types.BillingModePayPerRequest,
	}
	return utils.CreateTable(ctx, sdb.client, &createTableInput)
}

func (sdb *StateDatabase) GetCheckpoint(ctx context.Context, ckptType models.CheckpointType) (time.Time, error) {
	getItemIn := dynamodb.GetItemInput{
		Key: map[string]types.AttributeValue{
			"name": &types.AttributeValueMemberS{Value: string(ckptType)},
		},
		TableName: aws.String(sdb.checkpointTable),
	}

	httpCtx, httpCancel := context.WithTimeout(ctx, common.DefaultRpcWaitTime)
	defer httpCancel()

	getItemOut, err := sdb.client.GetItem(httpCtx, &getItemIn)
	if err != nil {
		return time.Time{}, err
	}
	if getItemOut.Item != nil {
		checkpoint := models.Checkpoint{}
		if err = attributevalue.UnmarshalMapWithOptions(getItemOut.Item, &checkpoint); err != nil {
			return time.Time{}, err
		}
		parsedCheckpoint, _ := time.Parse(common.DbDateFormat, checkpoint.Value)
		return parsedCheckpoint, nil
	}
	return time.Time{}, nil
}

func (sdb *StateDatabase) UpdateCheckpoint(ctx context.Context, checkpointType models.CheckpointType, checkpoint time.Time) (bool, error) {
	checkpointStr := checkpoint.Format(common.DbDateFormat)
	updateItemIn := dynamodb.UpdateItemInput{
		Key: map[string]types.AttributeValue{
			"name": &types.AttributeValueMemberS{Value: string(checkpointType)},
		},
		TableName:           aws.String(sdb.checkpointTable),
		ConditionExpression: aws.String("attribute_not_exists(#value) or :value > #value"),
		ExpressionAttributeNames: map[string]string{
			"#value": "value",
		},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":value": &types.AttributeValueMemberS{Value: checkpointStr},
		},
		UpdateExpression: aws.String("set #value = :value"),
	}

	httpCtx, httpCancel := context.WithTimeout(ctx, common.DefaultRpcWaitTime)
	defer httpCancel()

	if _, err := sdb.client.UpdateItem(httpCtx, &updateItemIn); err != nil {
		// To get a specific API error
		var condUpdErr *types.ConditionalCheckFailedException
		if errors.As(err, &condUpdErr) {
			// Not an error, just indicate that we couldn't update the entry
			sdb.logger.Errorf("error updating checkpoint: %s, %v", checkpointStr, err)
			return false, nil
		}
		sdb.logger.Errorf("error writing to db: %v", err)
		return false, err
	}
	return true, nil
}

func (sdb *StateDatabase) StoreCid(ctx context.Context, streamCid *models.StreamCid) (bool, error) {
	if attributeValues, err := attributevalue.MarshalMapWithOptions(streamCid); err != nil {
		return false, err
	} else {
		// Deduplicate CIDs
		putItemIn := dynamodb.PutItemInput{
			TableName:                aws.String(sdb.streamTable),
			ConditionExpression:      aws.String("attribute_not_exists(#id)"),
			ExpressionAttributeNames: map[string]string{"#id": "id"},
			Item:                     attributeValues,
		}

		httpCtx, httpCancel := context.WithTimeout(ctx, common.DefaultRpcWaitTime)
		defer httpCancel()

		if _, err = sdb.client.PutItem(httpCtx, &putItemIn); err != nil {
			// To get a specific API error
			var condUpdErr *types.ConditionalCheckFailedException
			if errors.As(err, &condUpdErr) {
				// Not an error, just indicate that we couldn't write the entry
				return false, nil
			}
			sdb.logger.Errorf("error writing to db: %v", err)
			return false, err
		}
		return true, nil
	}
}

func (sdb *StateDatabase) UpdateTip(ctx context.Context, newTip *models.StreamTip) (bool, *models.StreamTip, error) {
	if attributeValues, err := attributevalue.MarshalMapWithOptions(newTip, func(options *attributevalue.EncoderOptions) {
		options.EncodeTime = func(time time.Time) (types.AttributeValue, error) {
			return &types.AttributeValueMemberN{Value: strconv.FormatInt(time.UnixNano(), 10)}, nil
		}
	}); err != nil {
		return false, nil, err
	} else {
		// Deduplicate stream tips
		putItemIn := dynamodb.PutItemInput{
			TableName:                aws.String(sdb.tipTable),
			ConditionExpression:      aws.String("attribute_not_exists(#id) OR (#ts <= :ts)"),
			ExpressionAttributeNames: map[string]string{"#id": "id", "#ts": "ts"},
			ExpressionAttributeValues: map[string]types.AttributeValue{
				":ts": &types.AttributeValueMemberN{Value: strconv.FormatInt(newTip.Timestamp.UnixNano(), 10)},
			},
			Item:         attributeValues,
			ReturnValues: types.ReturnValueAllOld,
		}

		httpCtx, httpCancel := context.WithTimeout(ctx, common.DefaultRpcWaitTime)
		defer httpCancel()

		if putItemOut, err := sdb.client.PutItem(httpCtx, &putItemIn); err != nil {
			// To get a specific API error
			var condUpdErr *types.ConditionalCheckFailedException
			if errors.As(err, &condUpdErr) {
				// Not an error, just indicate that we couldn't write the entry.
				return false, nil, nil
			}
			sdb.logger.Errorf("error writing to db: %v", err)
			return false, nil, err
		} else if len(putItemOut.Attributes) > 0 {
			oldTip := new(models.StreamTip)
			if err = attributevalue.UnmarshalMapWithOptions(
				putItemOut.Attributes,
				oldTip,
				func(options *attributevalue.DecoderOptions) {
					options.DecodeTime = attributevalue.DecodeTimeAttributes{
						S: utils.TsDecode,
						N: utils.TsDecode,
					}
				}); err != nil {
				sdb.logger.Errorf("error unmarshalling old tip: %v", err)
				// We've written the new tip and lost the previous tip here. This means that we won't be able to mark
				// the old tip REPLACED. As a result, the old tip will get anchored along with the new tip, causing the
				// new tip to be rejected in Ceramic via conflict resolution. While not ideal, this is no worse than
				// what we have today.
				return false, nil, err
			}
			return true, oldTip, nil
		}
		// We wrote a new tip but did not have an old tip to return
		return true, nil, nil
	}
}
