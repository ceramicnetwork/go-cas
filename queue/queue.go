package queue

import (
	"context"
	"encoding/json"
	"fmt"
	"go/types"
	"log"
	"os"
	"strconv"
	"time"

	"github.com/abevier/tsk/batch"
	"github.com/abevier/tsk/futures"
	"github.com/abevier/tsk/ratelimiter"
	"github.com/abevier/tsk/results"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	sqsTypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"

	"github.com/smrz2001/go-cas/models"
)

type QueueType string

const (
	QueueType_Request QueueType = "request"
	QueueType_Ready   QueueType = "ready"
	QueueType_Worker  QueueType = "worker"
	QueueType_Failure QueueType = "failure"
)

const SqsBatchSize = 10
const SqsRateLimit = 250 // upto 2500 messages per second
const SqsVisibilityTimeout = 3 * time.Minute

type QueueMessage[T any] struct {
	Body              T
	Attributes        map[string]string
	MessageAttributes map[string]sqsTypes.MessageAttributeValue
	MessageId         *string
	ReceiptHandle     *string
}

type Queueable[T any] interface {
	GetMessageDeduplicationId() *string
	GetMessageGroupId() *string
}

type Queue[T Queueable[T]] struct {
	client         *sqs.Client
	txExecutor     *batch.BatchExecutor[T, string]
	txRateLimiter  *ratelimiter.RateLimiter[[]T, []results.Result[string]]
	rxRateLimiter  *ratelimiter.RateLimiter[types.Nil, []*QueueMessage[T]]
	delExecutor    *batch.BatchExecutor[string, string]
	delRateLimiter *ratelimiter.RateLimiter[[]string, []results.Result[string]]
	url            string
}

// TODO: Initialize with server context and use that for batch executors
func NewQueue[T Queueable[T]](cfg aws.Config, name string) *Queue[T] {
	ctx, cancel := context.WithTimeout(context.Background(), models.DefaultHttpWaitTime)
	defer cancel()

	client := sqs.NewFromConfig(cfg)
	getUrlIn := sqs.GetQueueUrlInput{
		QueueName:              aws.String(fmt.Sprintf("cas-anchor-%s-%s.fifo", os.Getenv("ENV"), name)),
		QueueOwnerAWSAccountId: aws.String(os.Getenv("ACCOUNT_ID")),
	}
	getUrlOut, err := client.GetQueueUrl(ctx, &getUrlIn)
	if err != nil {
		log.Fatalf("newQueue: failed to retrieve %s queue url: %v", name, err)
	}
	q := Queue[T]{
		client: client,
		url:    *getUrlOut.QueueUrl,
	}
	beOpts := batch.BatchOpts{
		MaxSize:   SqsBatchSize,
		MaxLinger: models.DefaultBatchMaxLinger,
	}
	rlOpts := ratelimiter.RateLimiterOpts{
		Limit:             SqsRateLimit,
		Burst:             SqsRateLimit,
		FullQueueStrategy: ratelimiter.BlockWhenFull,
	}
	// Have each batch executor go through a rate limiter to control throughput
	q.txExecutor = batch.NewExecutor[T, string](beOpts, func(messages []T) ([]results.Result[string], error) {
		return q.txRateLimiter.Submit(context.Background(), messages)
	})
	q.txRateLimiter = ratelimiter.New(rlOpts, func(ctx context.Context, messages []T) ([]results.Result[string], error) {
		return q.enqueueBatch(messages)
	})
	q.rxRateLimiter = ratelimiter.New(rlOpts, func(ctx context.Context, _ types.Nil) ([]*QueueMessage[T], error) {
		return q.dequeueBatch()
	})
	q.delExecutor = batch.NewExecutor[string, string](beOpts, func(receiptHandles []string) ([]results.Result[string], error) {
		return q.delRateLimiter.Submit(context.Background(), receiptHandles)
	})
	q.delRateLimiter = ratelimiter.New(rlOpts, func(ctx context.Context, receiptHandles []string) ([]results.Result[string], error) {
		return q.deleteBatch(receiptHandles)
	})
	return &q
}

func (q Queue[T]) EnqueueF(message T) *futures.Future[string] {
	return q.txExecutor.SubmitF(message)
}

func (q Queue[T]) Enqueue(ctx context.Context, message T) (string, error) {
	return q.EnqueueF(message).Get(ctx)
}

func (q Queue[T]) enqueueBatch(messages []T) ([]results.Result[string], error) {
	ctx, cancel := context.WithTimeout(context.Background(), models.DefaultHttpWaitTime)
	defer cancel()

	// We need to map back two levels of results, from the batch construction and the batch send.
	batchResults := make([]results.Result[string], len(messages))

	entries := make([]sqsTypes.SendMessageBatchRequestEntry, 0, len(messages))
	for idx, message := range messages {
		messageBody, err := json.Marshal(message)
		if err != nil {
			batchResults[idx] = results.New[string]("", err)
			continue
		}
		entries = append(entries, sqsTypes.SendMessageBatchRequestEntry{
			Id:                     aws.String(strconv.Itoa(idx)), // Use the loop index so we can map results back
			MessageBody:            aws.String(string(messageBody)),
			MessageDeduplicationId: message.GetMessageDeduplicationId(),
			MessageGroupId:         message.GetMessageGroupId(),
		})
	}
	sendMsgBatchIn := sqs.SendMessageBatchInput{
		Entries:  entries,
		QueueUrl: aws.String(q.url),
	}
	sendMsgBatchOut, err := q.client.SendMessageBatch(ctx, &sendMsgBatchIn)
	if err != nil {
		// Let the batch executor populate the error. We'll lose information about any marshaling errors from above, but
		// that's ok.
		return nil, err
	}
	for _, success := range sendMsgBatchOut.Successful {
		idx, _ := strconv.Atoi(*success.Id)
		batchResults[idx] = results.New[string](*success.MessageId, nil)
	}
	for _, failure := range sendMsgBatchOut.Failed {
		idx, _ := strconv.Atoi(*failure.Id)
		batchResults[idx] = results.New[string]("", fmt.Errorf("enqueueBatch: %s:%s", *failure.Code, *failure.Message))
	}
	return batchResults, nil
}

func (q Queue[T]) DequeueF() *futures.Future[[]*QueueMessage[T]] {
	return q.rxRateLimiter.SubmitF(context.Background(), types.Nil{})
}

func (q Queue[T]) Dequeue(ctx context.Context) ([]*QueueMessage[T], error) {
	return q.DequeueF().Get(ctx)
}

func (q Queue[T]) dequeueBatch() ([]*QueueMessage[T], error) {
	ctx, cancel := context.WithTimeout(context.Background(), models.DefaultHttpWaitTime)
	defer cancel()

	rxMsgIn := sqs.ReceiveMessageInput{
		QueueUrl:            aws.String(q.url),
		MaxNumberOfMessages: SqsBatchSize,
		VisibilityTimeout:   int32(SqsVisibilityTimeout.Seconds()),
		WaitTimeSeconds:     0,
	}
	rxMsgOut, err := q.client.ReceiveMessage(ctx, &rxMsgIn)
	if err != nil {
		return nil, err
	}
	msgs := make([]*QueueMessage[T], len(rxMsgOut.Messages))
	for idx, msg := range rxMsgOut.Messages {
		m := new(QueueMessage[T])
		err = json.Unmarshal([]byte(*msg.Body), &m.Body)
		if err != nil {
			return nil, fmt.Errorf("dequeue: failed to deserialize message: %v", err)
		}
		m.Attributes = msg.Attributes
		m.MessageAttributes = msg.MessageAttributes
		m.MessageId = msg.MessageId
		m.ReceiptHandle = msg.ReceiptHandle
		msgs[idx] = m
	}
	return msgs, nil
}

func (q Queue[T]) DeleteF(receiptHandle string) *futures.Future[string] {
	return q.delExecutor.SubmitF(receiptHandle)
}

func (q Queue[T]) Delete(ctx context.Context, receiptHandle string) (string, error) {
	return q.DeleteF(receiptHandle).Get(ctx)
}

func (q Queue[T]) deleteBatch(receiptHandles []string) ([]results.Result[string], error) {
	ctx, cancel := context.WithTimeout(context.Background(), models.DefaultHttpWaitTime)
	defer cancel()

	// We need to map back two levels of results, from the batch construction and the batch delete.
	batchResults := make([]results.Result[string], len(receiptHandles))

	entries := make([]sqsTypes.DeleteMessageBatchRequestEntry, 0, len(receiptHandles))
	for idx, rxId := range receiptHandles {
		entries = append(entries, sqsTypes.DeleteMessageBatchRequestEntry{
			Id:            aws.String(strconv.Itoa(idx)),
			ReceiptHandle: aws.String(rxId),
		})
	}
	deleteMsgBatchIn := sqs.DeleteMessageBatchInput{
		Entries:  entries,
		QueueUrl: aws.String(q.url),
	}
	deleteMsgBatchOut, err := q.client.DeleteMessageBatch(ctx, &deleteMsgBatchIn)
	if err != nil {
		return nil, err
	}
	for _, success := range deleteMsgBatchOut.Successful {
		idx, _ := strconv.Atoi(*success.Id)
		batchResults[idx] = results.New[string](*success.Id, nil)
	}
	for _, failure := range deleteMsgBatchOut.Failed {
		idx, _ := strconv.Atoi(*failure.Id)
		batchResults[idx] = results.New[string]("", fmt.Errorf("deleteBatch: %s:%s", *failure.Code, *failure.Message))
	}
	return batchResults, nil
}
