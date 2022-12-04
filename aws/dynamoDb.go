package aws

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

	"github.com/smrz2001/go-cas"
)

const IdPosIndex = "id-pos-index"
const FamIdIndex = "fam-id-index"

var _ cas.Database = &DynamoDb{}

type DynamoDb struct {
	client          *dynamodb.Client
	streamTable     string
	checkpointTable string
	cursor          int64
}

func NewDynamoDb(cfg aws.Config) cas.Database {
	env := os.Getenv("ENV")
	// Use override endpoint, if specified, so that we can store jobs locally, while hitting regular AWS endpoints for
	// other operations. This allows local testing without affecting CD manager instances running in AWS.
	customEndpoint := os.Getenv("DB_AWS_ENDPOINT")
	var err error
	if len(customEndpoint) > 0 {
		log.Printf("newDynamoDb: using custom dynamodb aws endpoint: %s", customEndpoint)
		cfg, err = ConfigWithOverride(customEndpoint)
		if err != nil {
			log.Fatalf("failed to create aws cfg: %v", err)
		}
	}
	tablePfx := "cas-anchor-" + env + "-"
	streamTable := tablePfx + "stream"
	checkpointTable := tablePfx + "checkpoint"
	db := &DynamoDb{
		dynamodb.NewFromConfig(cfg),
		streamTable,
		checkpointTable,
		0,
	}
	return db
}

func (db DynamoDb) GetCheckpoint(ckptType cas.CheckpointType) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), cas.DefaultHttpWaitTime)
	defer cancel()

	getItemIn := dynamodb.GetItemInput{
		Key: map[string]types.AttributeValue{
			"name": &types.AttributeValueMemberS{Value: string(ckptType)},
		},
		TableName: aws.String(db.checkpointTable),
	}
	getItemOut, err := db.client.GetItem(ctx, &getItemIn)
	if err != nil {
		return "", err
	}
	if getItemOut.Item != nil {
		checkpoint := new(cas.Checkpoint)
		if err = attributevalue.UnmarshalMapWithOptions(getItemOut.Item, checkpoint); err != nil {
			return "", err
		}
		return checkpoint.Value, nil
	}
	return "", nil
}

func (db DynamoDb) UpdateCheckpoint(ckptType cas.CheckpointType, ckptValue string) (bool, error) {
	ctx, cancel := context.WithTimeout(context.Background(), cas.DefaultHttpWaitTime)
	defer cancel()

	updateItemIn := dynamodb.UpdateItemInput{
		Key: map[string]types.AttributeValue{
			"name": &types.AttributeValueMemberS{Value: string(ckptType)},
		},
		TableName:           aws.String(db.checkpointTable),
		ConditionExpression: aws.String(":value > #value"),
		ExpressionAttributeNames: map[string]string{
			"#value": "value",
		},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":value": &types.AttributeValueMemberS{Value: ckptValue},
		},
		UpdateExpression: aws.String("set #value = :value"),
	}
	if _, err := db.client.UpdateItem(ctx, &updateItemIn); err != nil {
		// To get a specific API error
		var condUpdErr *types.ConditionalCheckFailedException
		if errors.As(err, &condUpdErr) {
			// Not an error, just indicate that we couldn't update the entry
			log.Printf("could not update checkpoint: %s, %v", ckptValue, err)
			return false, nil
		}
		log.Printf("error writing to db: %v", err)
		return false, err
	}
	return true, nil
}

func (db DynamoDb) GetCid(streamId, cid string) (*cas.StreamCid, error) {
	ctx, cancel := context.WithTimeout(context.Background(), cas.DefaultHttpWaitTime)
	defer cancel()

	getItemIn := dynamodb.GetItemInput{
		Key: map[string]types.AttributeValue{
			"id":  &types.AttributeValueMemberS{Value: streamId},
			"cid": &types.AttributeValueMemberS{Value: cid},
		},
		TableName: aws.String(db.streamTable),
	}
	getItemOut, err := db.client.GetItem(ctx, &getItemIn)
	if err != nil {
		return nil, err
	}
	if getItemOut.Item != nil {
		streamCid := new(cas.StreamCid)
		if err = attributevalue.UnmarshalMapWithOptions(getItemOut.Item, streamCid); err != nil {
			return nil, err
		}
		return streamCid, nil
	}
	return nil, nil
}

func (db DynamoDb) GetLatestCid(streamId string) (*cas.StreamCid, error) {
	ctx, cancel := context.WithTimeout(context.Background(), cas.DefaultHttpWaitTime)
	defer cancel()

	queryIn := dynamodb.QueryInput{
		TableName:                aws.String(db.streamTable),
		ExpressionAttributeNames: map[string]string{"#id": "id"},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":id": &types.AttributeValueMemberS{Value: streamId},
		},
		IndexName:              aws.String(IdPosIndex),
		KeyConditionExpression: aws.String("#id = :id"),
		Limit:                  aws.Int32(1),    // just one CID entry
		ScanIndexForward:       aws.Bool(false), // descending order to get newest CID entry
	}
	queryOut, err := db.client.Query(ctx, &queryIn)
	if err != nil {
		return nil, err
	}
	if len(queryOut.Items) > 0 {
		streamCid := new(cas.StreamCid)
		if err = attributevalue.UnmarshalMapWithOptions(queryOut.Items[0], streamCid); err != nil {
			return nil, err
		}
		return streamCid, nil
	}
	return nil, nil
}

func (db DynamoDb) CreateCid(streamCid *cas.StreamCid) (bool, error) {
	ctx, cancel := context.WithTimeout(context.Background(), cas.DefaultHttpWaitTime)
	defer cancel()

	updateItemIn := dynamodb.UpdateItemInput{
		Key: map[string]types.AttributeValue{
			"id":  &types.AttributeValueMemberS{Value: streamCid.Id},
			"cid": &types.AttributeValueMemberS{Value: streamCid.Cid},
		},
		TableName:           aws.String(db.streamTable),
		ConditionExpression: aws.String("attribute_not_exists(#cid)"),
		// Only write this entry if it didn't already exist. This doesn't save write DynamoDB throughput but it can be
		// used to avoid unnecessary Ceramic multi-queries for streams/CIDs we are already aware of.
		ExpressionAttributeNames: map[string]string{"#cid": "cid"},
	}
	if _, err := db.client.UpdateItem(ctx, &updateItemIn); err != nil {
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

func (db DynamoDb) UpdateCid(streamCid *cas.StreamCid) error {
	if attributeValues, err := attributevalue.MarshalMapWithOptions(streamCid, func(options *attributevalue.EncoderOptions) {
		options.EncodeTime = func(time time.Time) (types.AttributeValue, error) {
			return &types.AttributeValueMemberN{Value: strconv.FormatInt(time.UnixMilli(), 10)}, nil
		}
	}); err != nil {
		return err
	} else {
		ctx, cancel := context.WithTimeout(context.Background(), cas.DefaultHttpWaitTime)
		defer cancel()

		_, err = db.client.PutItem(ctx, &dynamodb.PutItemInput{
			TableName: aws.String(db.streamTable),
			Item:      attributeValues,
		})
		return err
	}
}
