package ddb

import (
	"context"
	"log"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"github.com/ceramicnetwork/go-cas/models"
)

const tableCreationRetries = 3
const tableCreationWait = 3 * time.Second

func createTable(client *dynamodb.Client, createTableIn *dynamodb.CreateTableInput) error {
	if exists, err := tableExists(client, *createTableIn.TableName); !exists {
		ctx, cancel := context.WithTimeout(context.Background(), models.DefaultHttpWaitTime)
		defer cancel()

		if _, err = client.CreateTable(ctx, createTableIn); err != nil {
			return err
		}
		for i := 0; i < tableCreationRetries; i++ {
			if exists, err = tableExists(client, *createTableIn.TableName); exists {
				return nil
			}
			time.Sleep(tableCreationWait)
		}
		return err
	}
	return nil
}

func tableExists(client *dynamodb.Client, table string) (bool, error) {
	ctx, cancel := context.WithTimeout(context.Background(), models.DefaultHttpWaitTime)
	defer cancel()

	if output, err := client.DescribeTable(ctx, &dynamodb.DescribeTableInput{TableName: aws.String(table)}); err != nil {
		log.Printf("dynamodb: table does not exist: %v", table)
		return false, err
	} else {
		return output.Table.TableStatus == types.TableStatusActive, nil
	}
}
