package queue

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"

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
const SqsVisibilityTimeout = 3 * time.Minute

type QueueMessage[T any] struct {
	Body              T
	Attributes        map[string]string
	MessageAttributes map[string]types.MessageAttributeValue
	MessageId         *string
	ReceiptHandle     *string
}

type Queueable[T any] interface {
	GetMessageDeduplicationId() *string
	GetMessageGroupId() *string
}

type Queue[T Queueable[T]] struct {
	client *sqs.Client
	url    string
}

func NewQueue[T Queueable[T]](cfg aws.Config, name string) *Queue[T] {
	ctx, cancel := context.WithTimeout(context.Background(), models.DefaultHttpWaitTime)
	defer cancel()

	client := sqs.NewFromConfig(cfg)
	getUrlIn := sqs.GetQueueUrlInput{
		QueueName:              aws.String(fmt.Sprintf("cas-anchor-%s-%s.fifo", os.Getenv("ENV"), name)),
		QueueOwnerAWSAccountId: aws.String(os.Getenv("ACCOUNT_ID")),
	}
	getUrlOut, err := client.GetQueueUrl(ctx, &getUrlIn)
	// TODO: Create queue if it doesn't exist
	if err != nil {
		log.Fatalf("newQueue: failed to retrieve %s queue url: %v", name, err)
	}
	return &Queue[T]{
		client,
		*getUrlOut.QueueUrl,
	}
}

func (q Queue[T]) Enqueue(message T) (string, error) {
	// TODO: Use batch executor to consolidate SQS calls since we're charged per API call.
	ctx, cancel := context.WithTimeout(context.Background(), models.DefaultHttpWaitTime)
	defer cancel()

	messageBody, err := json.Marshal(message)
	if err != nil {
		return "", fmt.Errorf("enqueue: failed to serialize message: %v", err)
	}
	sendMsgIn := sqs.SendMessageInput{
		MessageBody:            aws.String(string(messageBody)),
		QueueUrl:               aws.String(q.url),
		MessageDeduplicationId: message.GetMessageDeduplicationId(),
		MessageGroupId:         message.GetMessageGroupId(),
	}
	sendMsgOut, err := q.client.SendMessage(ctx, &sendMsgIn)
	if err != nil {
		return "", err
	}
	return *sendMsgOut.MessageId, nil
}

func (q Queue[T]) Dequeue() ([]*QueueMessage[T], error) {
	ctx, cancel := context.WithTimeout(context.Background(), models.DefaultHttpWaitTime)
	defer cancel()

	rxMsgIn := sqs.ReceiveMessageInput{
		QueueUrl:            aws.String(q.url),
		MaxNumberOfMessages: SqsBatchSize,
		VisibilityTimeout:   int32(SqsVisibilityTimeout.Seconds()),
		// TODO: Use WaitTimeSeconds
		WaitTimeSeconds: 0,
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

func (q Queue[T]) Ack(rxId string) error {
	ctx, cancel := context.WithTimeout(context.Background(), models.DefaultHttpWaitTime)
	defer cancel()

	deleteMsgIn := sqs.DeleteMessageInput{
		QueueUrl:      aws.String(q.url),
		ReceiptHandle: aws.String(rxId),
	}
	_, err := q.client.DeleteMessage(ctx, &deleteMsgIn)
	if err != nil {
		return err
	}
	return nil
}
