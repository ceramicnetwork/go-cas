package db

import (
	"context"
	"errors"
	"log"
	"os"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	casAws "github.com/smrz2001/go-cas/aws"
	"github.com/smrz2001/go-cas/models"
)

const IdPosIndex = "id-pos-index"
const FamIdIndex = "fam-id-index"

type StateDatabase struct {
	client          *dynamodb.Client
	streamTable     string
	checkpointTable string
	cursor          int64
}

func NewStateDb(cfg aws.Config) *StateDatabase {
	env := os.Getenv("ENV")
	// Use override endpoint, if specified, so that we can store jobs locally, while hitting regular AWS endpoints for
	// other operations. This allows local testing without affecting CD manager instances running in AWS.
	customEndpoint := os.Getenv("DB_AWS_ENDPOINT")
	var err error
	if len(customEndpoint) > 0 {
		log.Printf("newStateDb: using custom dynamodb aws endpoint: %s", customEndpoint)
		cfg, err = casAws.ConfigWithOverride(customEndpoint)
		if err != nil {
			log.Fatalf("failed to create aws cfg: %v", err)
		}
	}
	tablePfx := "cas-anchor-" + env + "-"
	streamTable := tablePfx + "stream"
	checkpointTable := tablePfx + "checkpoint"
	db := &StateDatabase{
		dynamodb.NewFromConfig(cfg),
		streamTable,
		checkpointTable,
		0,
	}
	return db
}

func (sdb StateDatabase) GetCheckpoint(ckptType models.CheckpointType) (time.Time, error) {
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

func (sdb StateDatabase) UpdateCheckpoint(ckptType models.CheckpointType, checkpoint time.Time) (bool, error) {
	ctx, cancel := context.WithTimeout(context.Background(), models.DefaultHttpWaitTime)
	defer cancel()

	checkpointStr := checkpoint.Format(models.DbDateFormat)
	updateItemIn := dynamodb.UpdateItemInput{
		Key: map[string]types.AttributeValue{
			"name": &types.AttributeValueMemberS{Value: string(ckptType)},
		},
		TableName:           aws.String(sdb.checkpointTable),
		ConditionExpression: aws.String(":value > #value"),
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
			log.Printf("could not update checkpoint: %s, %v", checkpointStr, err)
			return false, nil
		}
		log.Printf("error writing to db: %v", err)
		return false, err
	}
	return true, nil
}

func (sdb StateDatabase) GetCid(streamId, cid string) (*models.StreamCid, error) {
	ctx, cancel := context.WithTimeout(context.Background(), models.DefaultHttpWaitTime)
	defer cancel()

	getItemIn := dynamodb.GetItemInput{
		Key: map[string]types.AttributeValue{
			"id":  &types.AttributeValueMemberS{Value: streamId},
			"cid": &types.AttributeValueMemberS{Value: cid},
		},
		TableName: aws.String(sdb.streamTable),
	}
	getItemOut, err := sdb.client.GetItem(ctx, &getItemIn)
	if err != nil {
		return nil, err
	}
	if getItemOut.Item != nil {
		streamCid := models.StreamCid{}
		if err = attributevalue.UnmarshalMapWithOptions(getItemOut.Item, &streamCid); err != nil {
			return nil, err
		}
		return &streamCid, nil
	}
	return nil, nil
}

func (sdb StateDatabase) GetLatestCid(streamId string) (*models.StreamCid, error) {
	ctx, cancel := context.WithTimeout(context.Background(), models.DefaultHttpWaitTime)
	defer cancel()

	queryIn := dynamodb.QueryInput{
		TableName:                aws.String(sdb.streamTable),
		ExpressionAttributeNames: map[string]string{"#id": "id"},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":id": &types.AttributeValueMemberS{Value: streamId},
		},
		IndexName:              aws.String(IdPosIndex),
		KeyConditionExpression: aws.String("#id = :id"),
		Limit:                  aws.Int32(1),    // just one CID entry
		ScanIndexForward:       aws.Bool(false), // descending order to get newest CID entry
	}
	queryOut, err := sdb.client.Query(ctx, &queryIn)
	if err != nil {
		return nil, err
	}
	if len(queryOut.Items) > 0 {
		streamCid := models.StreamCid{}
		if err = attributevalue.UnmarshalMapWithOptions(queryOut.Items[0], &streamCid); err != nil {
			return nil, err
		}
		return &streamCid, nil
	}
	return nil, nil
}

func (sdb StateDatabase) CreateCid(streamCid *models.StreamCid) (bool, error) {
	ctx, cancel := context.WithTimeout(context.Background(), models.DefaultHttpWaitTime)
	defer cancel()

	updateItemIn := dynamodb.UpdateItemInput{
		Key: map[string]types.AttributeValue{
			"id":  &types.AttributeValueMemberS{Value: streamCid.Id},
			"cid": &types.AttributeValueMemberS{Value: streamCid.Cid},
		},
		TableName:           aws.String(sdb.streamTable),
		ConditionExpression: aws.String("attribute_not_exists(#cid)"),
		// Only write this entry if it didn't already exist. This doesn't save write DynamoDB throughput but it can be
		// used to avoid unnecessary Ceramic multi-queries for streams/CIDs we are already aware of.
		ExpressionAttributeNames: map[string]string{"#cid": "cid"},
	}
	if _, err := sdb.client.UpdateItem(ctx, &updateItemIn); err != nil {
		// To get a specific API error
		var condUpdErr *types.ConditionalCheckFailedException
		if errors.As(err, &condUpdErr) {
			// Not an error, just indicate that we couldn't update the entry
			return false, nil
		}
		log.Printf("error writing to db: %v", err)
		return false, err
	}
	return true, nil
}

func (sdb StateDatabase) UpdateCid(streamCid *models.StreamCid) error {
	if attributeValues, err := attributevalue.MarshalMapWithOptions(streamCid); err != nil {
		return err
	} else {
		ctx, cancel := context.WithTimeout(context.Background(), models.DefaultHttpWaitTime)
		defer cancel()

		_, err = sdb.client.PutItem(ctx, &dynamodb.PutItemInput{
			TableName: aws.String(sdb.streamTable),
			Item:      attributeValues,
		})
		return err
	}
}
