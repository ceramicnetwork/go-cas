package migrate

import (
	"context"
	"log"
	"os"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"github.com/smrz2001/go-cas/aws"
	"github.com/smrz2001/go-cas/models"
)

type Database struct {
	client       *dynamodb.Client
	requestTable string
}

type batchRequest struct {
	Checkpoint time.Time `dynamodbav:"ckpt"`
	RequestId  string    `dynamodbav:"id"`
	Ts         time.Time `dynamodbav:"ts"`
}

func NewMigrationDb() *Database {
	cfg, err := cas.AwsConfig()
	if err != nil {
		log.Fatalf("migration: failed to create aws cfg: %v", err)
	}
	return &Database{
		dynamodb.NewFromConfig(cfg),
		"cas-anchor-" + os.Getenv("ENV") + "-request",
	}
}

// IterateBatch iterates through all the entries in a particular batch
func (mdb Database) IterateBatch(checkpoint time.Time, asc bool, iter func(batchRequest) (bool, error)) (bool, error) {
	return mdb.iterateRequests(&dynamodb.QueryInput{
		TableName:              aws.String(mdb.requestTable),
		KeyConditionExpression: aws.String("#checkpoint = :checkpoint"),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":checkpoint": mdb.tsEncode(checkpoint),
		},
		ExpressionAttributeNames: map[string]string{
			"#checkpoint": "ckpt",
		},
		ScanIndexForward: aws.Bool(asc),
	}, iter)
}

func (mdb Database) tsEncode(ts time.Time) *types.AttributeValueMemberN {
	return &types.AttributeValueMemberN{Value: strconv.FormatInt(ts.UnixMicro(), 10)}
}

func (mdb Database) tsDecodeOpts(options *attributevalue.DecoderOptions) {
	decodeFn := func(ts string) (time.Time, error) {
		usec, err := strconv.ParseInt(ts, 10, 64)
		if err != nil {
			return time.Time{}, err
		}
		return time.UnixMicro(usec), nil
	}
	options.DecodeTime = attributevalue.DecodeTimeAttributes{
		S: decodeFn,
		N: decodeFn,
	}
}

func (mdb Database) iterateRequests(queryInput *dynamodb.QueryInput, iter func(batchRequest) (bool, error)) (bool, error) {
	p := dynamodb.NewQueryPaginator(mdb.client, queryInput)
	foundItems := false
	for p.HasMorePages() {
		err := func() error {
			ctx, cancel := context.WithTimeout(context.Background(), models.DefaultHttpWaitTime)
			defer cancel()

			if page, err := p.NextPage(ctx); err != nil {
				return err
			} else {
				var batchRequests []batchRequest
				if err = attributevalue.UnmarshalListOfMapsWithOptions(page.Items, &batchRequests, mdb.tsDecodeOpts); err != nil {
					log.Printf("iterateRequests: unable to unmarshal requests: %v", err)
					return err
				}
				for _, batchReq := range batchRequests {
					foundItems = true
					if iterate, err := iter(batchReq); err != nil {
						return err
					} else if !iterate {
						return nil
					}
				}
				return nil
			}
		}()
		if err != nil {
			return false, err
		}
	}
	return foundItems, nil
}

func (mdb Database) WriteRequest(checkpoint time.Time, requestId string) error {
	attributeValues, err := attributevalue.MarshalMapWithOptions(
		batchRequest{checkpoint, requestId, time.Now()},
		func(options *attributevalue.EncoderOptions) {
			options.EncodeTime = func(time time.Time) (types.AttributeValue, error) {
				return &types.AttributeValueMemberN{Value: strconv.FormatInt(time.UnixMicro(), 10)}, nil
			}
		},
	)
	if err != nil {
		return err
	} else {
		ctx, cancel := context.WithTimeout(context.Background(), models.DefaultHttpWaitTime)
		defer cancel()

		if _, err = mdb.client.PutItem(ctx, &dynamodb.PutItemInput{
			TableName: aws.String(mdb.requestTable),
			Item:      attributeValues,
		}); err != nil {
			return err
		}
	}
	return nil
}

func (mdb Database) DeleteRequest(checkpoint time.Time, requestId string) error {
	ctx, cancel := context.WithTimeout(context.Background(), models.DefaultHttpWaitTime)
	defer cancel()

	deleteItemIn := dynamodb.DeleteItemInput{
		TableName: aws.String(mdb.requestTable),
		Key: map[string]types.AttributeValue{
			"ckpt": mdb.tsEncode(checkpoint),
			"id":   &types.AttributeValueMemberS{Value: requestId},
		},
	}
	if _, err := mdb.client.DeleteItem(ctx, &deleteItemIn); err != nil {
		return err
	}
	return nil
}
