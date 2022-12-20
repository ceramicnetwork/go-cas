# Go Ceramic Anchor Service

## Architecture

### Data Pipeline

https://lucid.app/documents/view/c9b677a7-2b60-4bec-89c7-43d10e2a262e

### Flows

https://lucid.app/documents/view/9fb65517-add6-48f5-a35b-e8c5835a9762

## TODOs (roughly ordered by priority)

### Major

- [ ] Implement batching service (with in-memory batch cache): `Ready` queue -> `Worker` / `Failure` queue
- [ ] Implement failure handling service: `Failure` queue -> ?
- [ ] Use [go-sqs](https://github.com/ABevier/go-sqs) for scaling up SQS processing
- [ ] Abstract pattern for "Service" - going to need state DB + ingress/egress queues for 3 services
- [ ] Implement [dead-letter queue (DLQ)](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-dead-letter-queues.html)
- [ ] Unit tests (backward compatibility tests?)
- [ ] Performance tests
- [ ] Metrics

### Minor

- [ ] CI/CD

### Starter

- [ ] Create SQS queue if it doesn't exist
- [ ] Create DynamoDB table if it doesn't exist (see [pipeline-tools](https://github.com/3box/pipeline-tools/blob/develop/cd/manager/aws/dynamoDb.go#L62))
