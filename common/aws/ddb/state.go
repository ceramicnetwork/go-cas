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

const stateIdTsIndex = "id-ts-index"
const stateIdAnchorTsIndex = "id-ats-index"

type StateDatabase struct {
	client          *dynamodb.Client
	streamTable     string
	checkpointTable string
}

func NewStateDb(client *dynamodb.Client) *StateDatabase {
	env := os.Getenv("ENV")

	tablePfx := "cas-anchor-" + env + "-"
	streamTable := tablePfx + "stream"
	checkpointTable := tablePfx + "checkpoint"

	sdb := StateDatabase{
		client,
		streamTable,
		checkpointTable,
	}

	if err := sdb.createCheckpointTable(); err != nil {
		log.Fatalf("state: checkpoint table creation failed: %v", err)
	}
	if err := sdb.createStreamTable(); err != nil {
		log.Fatalf("state: stream table creation failed: %v", err)
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
	createStreamTableInput := dynamodb.CreateTableInput{
		AttributeDefinitions: []types.AttributeDefinition{
			{
				AttributeName: aws.String("id"),
				AttributeType: "S",
			},
			{
				AttributeName: aws.String("cid"),
				AttributeType: "S",
			},
			{
				AttributeName: aws.String("ts"),
				AttributeType: "N",
			},
			{
				AttributeName: aws.String("ats"),
				AttributeType: "N",
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
		GlobalSecondaryIndexes: []types.GlobalSecondaryIndex{
			{
				IndexName: aws.String(stateIdTsIndex),
				KeySchema: []types.KeySchemaElement{
					{
						AttributeName: aws.String("id"),
						KeyType:       "HASH",
					},
					{
						AttributeName: aws.String("ts"),
						KeyType:       "RANGE",
					},
				},
				Projection: &types.Projection{
					ProjectionType: types.ProjectionTypeAll,
				},
				ProvisionedThroughput: &types.ProvisionedThroughput{
					ReadCapacityUnits:  aws.Int64(1),
					WriteCapacityUnits: aws.Int64(1),
				},
			},
			{
				IndexName: aws.String(stateIdAnchorTsIndex),
				KeySchema: []types.KeySchemaElement{
					{
						AttributeName: aws.String("id"),
						KeyType:       "HASH",
					},
					{
						AttributeName: aws.String("ats"),
						KeyType:       "RANGE",
					},
				},
				Projection: &types.Projection{
					ProjectionType: types.ProjectionTypeAll,
				},
				ProvisionedThroughput: &types.ProvisionedThroughput{
					ReadCapacityUnits:  aws.Int64(1),
					WriteCapacityUnits: aws.Int64(1),
				},
			},
		},
	}
	return createTable(sdb.client, &createStreamTableInput)
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

func (sdb *StateDatabase) GetCid(streamId, cid string) (*models.StreamCid, error) {
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

func (sdb *StateDatabase) GetTipCid(streamId string) (*models.StreamCid, error) {
	var latest *models.StreamCid = nil
	if err := sdb.iterateCids(
		&dynamodb.QueryInput{
			TableName:              aws.String(sdb.streamTable),
			IndexName:              aws.String(stateIdTsIndex),
			KeyConditionExpression: aws.String("#id = :id"),
			ExpressionAttributeValues: map[string]types.AttributeValue{
				":id": &types.AttributeValueMemberS{Value: streamId},
			},
			ExpressionAttributeNames: map[string]string{
				"#id": "id",
			},
			ScanIndexForward: aws.Bool(false), // descending
		},
		func(streamCid *models.StreamCid) bool {
			latest = streamCid
			return false // stop iteration after the first entry
		},
	); err != nil {
		return nil, err
	}
	return latest, nil
}

func (sdb *StateDatabase) UpdateAnchorTs(streamId, cid string, anchorTs time.Time) (bool, error) {
	ctx, cancel := context.WithTimeout(context.Background(), models.DefaultHttpWaitTime)
	defer cancel()

	updateItemIn := dynamodb.UpdateItemInput{
		Key: map[string]types.AttributeValue{
			"id":  &types.AttributeValueMemberS{Value: streamId},
			"cid": &types.AttributeValueMemberS{Value: cid},
		},
		TableName:           aws.String(sdb.streamTable),
		ConditionExpression: aws.String("attribute_not_exists(#ancTs)"),
		ExpressionAttributeNames: map[string]string{
			"#ancTs": "ats",
		},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":ancTs": &types.AttributeValueMemberN{Value: strconv.FormatInt(anchorTs.Unix(), 10)},
		},
		UpdateExpression: aws.String("set #ancTs = :ancTs"),
	}
	if _, err := sdb.client.UpdateItem(ctx, &updateItemIn); err != nil {
		// To get a specific API error
		var condUpdErr *types.ConditionalCheckFailedException
		if errors.As(err, &condUpdErr) {
			// Not an error, just indicate that we couldn't update the entry
			log.Printf("updateAnchorTs: anchor timestamp already set: %s, %s, %s, %v", streamId, cid, anchorTs, err)
			return false, nil
		}
		log.Printf("updateAnchorTs: error writing to db: %v", err)
		return false, err
	}
	return true, nil
}

func (sdb *StateDatabase) GetAnchoredCid(streamId, cid string) (*models.StreamCid, error) {
	if streamCid, err := sdb.GetCid(streamId, cid); err != nil {
		return nil, err
	} else if streamCid.AnchorTs != nil {
		return streamCid, nil
	} else {
		var anchoredCid *models.StreamCid = nil
		if err = sdb.iterateCids(
			&dynamodb.QueryInput{
				TableName:              aws.String(sdb.streamTable),
				IndexName:              aws.String(stateIdAnchorTsIndex),
				KeyConditionExpression: aws.String("#id = :id and #ancTs >= :ts"),
				ExpressionAttributeValues: map[string]types.AttributeValue{
					":id": &types.AttributeValueMemberS{Value: streamId},
					":ts": &types.AttributeValueMemberN{Value: strconv.FormatInt(streamCid.Timestamp.Unix(), 10)},
				},
				ExpressionAttributeNames: map[string]string{
					"#id":    "id",
					"#ancTs": "ats",
				},
				ScanIndexForward: aws.Bool(true), // ascending
			},
			func(streamCid *models.StreamCid) bool {
				anchoredCid = streamCid
				return false // stop iteration after the first entry
			},
		); err != nil {
			return nil, err
		}
		return anchoredCid, nil
	}
}

func (sdb *StateDatabase) iterateCids(queryInput *dynamodb.QueryInput, iter func(*models.StreamCid) bool) error {
	p := dynamodb.NewQueryPaginator(sdb.client, queryInput)
	for p.HasMorePages() {
		err := func() error {
			ctx, cancel := context.WithTimeout(context.Background(), models.DefaultHttpWaitTime)
			defer cancel()

			page, err := p.NextPage(ctx)
			if err != nil {
				return err
			}
			var streamCidPage []*models.StreamCid
			err = attributevalue.UnmarshalListOfMapsWithOptions(page.Items, &streamCidPage)
			if err != nil {
				log.Printf("iterateCids: unable to unmarshal entry: %v", err)
				return err
			}
			for _, streamCid := range streamCidPage {
				if !iter(streamCid) {
					return nil
				}
			}
			return nil
		}()
		if err != nil {
			return err
		}
	}
	return nil
}
