package storage

import (
	"bytes"
	"context"
	"encoding/json"
	"os"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/ceramicnetwork/go-cas"
	"github.com/ceramicnetwork/go-cas/common"
	"github.com/ceramicnetwork/go-cas/models"
)

var _ models.KeyValueRepository = &S3Store{}

type S3Store struct {
	client *s3.Client
	logger models.Logger
	bucket string
}

func NewS3Store(logger models.Logger, s3Client *s3.Client) *S3Store {
	bucket := "ceramic-" + os.Getenv(cas.Env_Env) + "-cas"
	return &S3Store{s3Client, logger, bucket}
}

func (s *S3Store) Store(ctx context.Context, key string, value interface{}) error {
	if jsonBytes, err := json.Marshal(value); err != nil {
		return err
	} else {
		httpCtx, httpCancel := context.WithTimeout(ctx, common.DefaultRpcWaitTime)
		defer httpCancel()

		putObjectIn := s3.PutObjectInput{
			Bucket:      aws.String(s.bucket),
			Key:         aws.String(key),
			Body:        bytes.NewReader(jsonBytes),
			ContentType: aws.String("application/json"),
		}
		if _, err = s.client.PutObject(httpCtx, &putObjectIn); err != nil {
			return err
		} else {
			s.logger.Debugf("stored key: %s", key)
		}
	}
	return nil
}
