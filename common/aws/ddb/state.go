package ddb

import (
	"context"
	"errors"
	"log"
	"os"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"github.com/ceramicnetwork/go-cas/models"
)

type StateDatabase struct {
	client          *dynamodb.Client
	checkpointTable string
	streamTable     string
	tipTable        string
}

func NewStateDb(client *dynamodb.Client) *StateDatabase {
	env := os.Getenv("ENV")

	tablePfx := "cas-anchor-" + env + "-"
	checkpointTable := tablePfx + "checkpoint"
	streamTable := tablePfx + "stream"
	tipTable := tablePfx + "tip"

	sdb := StateDatabase{
		client,
		checkpointTable,
		streamTable,
		tipTable,
	}
	if err := sdb.createCheckpointTable(); err != nil {
		log.Fatalf("state: checkpoint table creation failed: %v", err)
	} else if err = sdb.createStreamTable(); err != nil {
		log.Fatalf("state: stream table creation failed: %v", err)
	} else if err = sdb.createTipTable(); err != nil {
		log.Fatalf("state: tip table creation failed: %v", err)
	}
	return &sdb
}

func (sdb *StateDatabase) createCheckpointTable() error {
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
		TableName: aws.String(sdb.checkpointTable),
		ProvisionedThroughput: &types.ProvisionedThroughput{
			ReadCapacityUnits:  aws.Int64(1),
			WriteCapacityUnits: aws.Int64(1),
		},
	}
	return createTable(sdb.client, &createStreamTableInput)
}

func (sdb *StateDatabase) createStreamTable() error {
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
		TableName: aws.String(sdb.streamTable),
		ProvisionedThroughput: &types.ProvisionedThroughput{
			ReadCapacityUnits:  aws.Int64(1),
			WriteCapacityUnits: aws.Int64(1),
		},
	}
	return createTable(sdb.client, &createTableInput)
}

func (sdb *StateDatabase) createTipTable() error {
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
		TableName: aws.String(sdb.tipTable),
		ProvisionedThroughput: &types.ProvisionedThroughput{
			ReadCapacityUnits:  aws.Int64(1),
			WriteCapacityUnits: aws.Int64(1),
		},
	}
	return createTable(sdb.client, &createTableInput)
}

func (sdb *StateDatabase) GetCheckpoint(ckptType models.CheckpointType) (time.Time, error) {
	ctx, cancel := context.WithTimeout(context.Background(), models.DefaultHttpWaitTime)
	defer cancel()

	getItemIn := dynamodb.GetItemInput{
		Key: map[string]types.AttributeValue{
			"name": &types.AttributeValueMemberS{Value: string(ckptType)},
		},
		TableName: aws.String(sdb.checkpointTable),
	}
	getItemOut, err := sdb.client.GetItem(ctx, &getItemIn)
	if err != nil {
		return time.Time{}, err
	}
	if getItemOut.Item != nil {
		checkpoint := models.Checkpoint{}
		if err = attributevalue.UnmarshalMapWithOptions(getItemOut.Item, &checkpoint); err != nil {
			return time.Time{}, err
		}
		parsedCheckpoint, _ := time.Parse(models.DbDateFormat, checkpoint.Value)
		return parsedCheckpoint, nil
	}
	return time.Time{}, nil
}

func (sdb *StateDatabase) UpdateCheckpoint(checkpointType models.CheckpointType, checkpoint time.Time) (bool, error) {
	ctx, cancel := context.WithTimeout(context.Background(), models.DefaultHttpWaitTime)
	defer cancel()

	checkpointStr := checkpoint.Format(models.DbDateFormat)
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
	if _, err := sdb.client.UpdateItem(ctx, &updateItemIn); err != nil {
		// To get a specific API error
		var condUpdErr *types.ConditionalCheckFailedException
		if errors.As(err, &condUpdErr) {
			// Not an error, just indicate that we couldn't update the entry
			log.Printf("updateCheckpoint: could not update checkpoint: %s, %v", checkpointStr, err)
			return false, nil
		}
		log.Printf("updateCheckpoint: error writing to db: %v", err)
		return false, err
	}
	return true, nil
}

func (sdb *StateDatabase) StoreCid(streamCid *models.StreamCid) (bool, error) {
	if attributeValues, err := attributevalue.MarshalMapWithOptions(streamCid); err != nil {
		return false, err
	} else {
		ctx, cancel := context.WithTimeout(context.Background(), models.DefaultHttpWaitTime)
		defer cancel()

		// Deduplicate CIDs
		putItemIn := dynamodb.PutItemInput{
			TableName:                aws.String(sdb.streamTable),
			ConditionExpression:      aws.String("attribute_not_exists(#id)"),
			ExpressionAttributeNames: map[string]string{"#id": "id"},
			Item:                     attributeValues,
		}
		if _, err = sdb.client.PutItem(ctx, &putItemIn); err != nil {
			// To get a specific API error
			var condUpdErr *types.ConditionalCheckFailedException
			if errors.As(err, &condUpdErr) {
				// Not an error, just indicate that we couldn't write the entry
				return false, nil
			}
			log.Printf("storeCid: error writing to db: %v", err)
			return false, err
		}
		return true, nil
	}
}

func (sdb *StateDatabase) UpdateTip(newTip *models.StreamTip) (bool, *models.StreamTip, error) {
	if attributeValues, err := attributevalue.MarshalMapWithOptions(newTip, func(options *attributevalue.EncoderOptions) {
		options.EncodeTime = func(time time.Time) (types.AttributeValue, error) {
			return &types.AttributeValueMemberN{Value: strconv.FormatInt(time.UnixMilli(), 10)}, nil
		}
	}); err != nil {
		return false, nil, err
	} else {
		ctx, cancel := context.WithTimeout(context.Background(), models.DefaultHttpWaitTime)
		defer cancel()

		// Deduplicate stream tips
		putItemIn := dynamodb.PutItemInput{
			TableName:                aws.String(sdb.tipTable),
			ConditionExpression:      aws.String("attribute_not_exists(#id) OR (#ts <= :ts)"),
			ExpressionAttributeNames: map[string]string{"#id": "id", "#ts": "ts"},
			ExpressionAttributeValues: map[string]types.AttributeValue{
				":ts": &types.AttributeValueMemberN{Value: strconv.FormatInt(newTip.Timestamp.UnixMilli(), 10)},
			},
			Item:         attributeValues,
			ReturnValues: types.ReturnValueAllOld,
		}
		if putItemOut, err := sdb.client.PutItem(ctx, &putItemIn); err != nil {
			// To get a specific API error
			var condUpdErr *types.ConditionalCheckFailedException
			if errors.As(err, &condUpdErr) {
				// Not an error, just indicate that we couldn't write the entry.
				return false, nil, nil
			}
			log.Printf("updateTip: error writing to db: %v", err)
			return false, nil, err
		} else if len(putItemOut.Attributes) > 0 {
			oldTip := new(models.StreamTip)
			if err = attributevalue.UnmarshalMapWithOptions(
				putItemOut.Attributes,
				oldTip,
				func(options *attributevalue.DecoderOptions) {
					options.DecodeTime = attributevalue.DecodeTimeAttributes{
						S: sdb.tsDecode,
						N: sdb.tsDecode,
					}
				}); err != nil {
				log.Printf("updateTip: error unmarshaling old tip: %v", err)
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

func (sdb *StateDatabase) tsDecode(ts string) (time.Time, error) {
	msec, err := strconv.ParseInt(ts, 10, 64)
	if err != nil {
		return time.Time{}, err
	}
	return time.UnixMilli(msec), nil
}
