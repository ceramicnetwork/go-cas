# Go Ceramic Anchor Service

## Architecture

### Data Pipeline

https://lucid.app/documents/view/c9b677a7-2b60-4bec-89c7-43d10e2a262e

### Flows

https://lucid.app/documents/view/9fb65517-add6-48f5-a35b-e8c5835a9762

## TODOs (roughly ordered by priority)

### Major

- [ ] Implement batching service
- [ ] Implement failure handling service
- [ ] Prepare Anchor Worker code to accept batches
- [ ] Prepare Anchor Worker code to post anchor results to a queue
- [ ] CD manager to inject batches into workers
- [ ] Remove Ceramic loading and pinning
- [ ] Terraform for deployments
- [ ] Implement [dead-letter queue (DLQ)](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-dead-letter-queues.html)
- [ ] Metrics
- [ ] Unit tests (backward compatibility tests?)
- [ ] Performance tests

### Minor

- [ ] Test against local infra, e.g. [Localstack](https://localstack.cloud/)
- [ ] CI/CD
- [ ] Clarify contexts being used in various spots - operation ctx vs. server ctx

### Starter

- [ ] Create SQS queue if it doesn't exist.
- [ ] Create DynamoDB table if it doesn't exist (see [pipeline-tools](https://github.com/3box/pipeline-tools/blob/develop/cd/manager/aws/dynamoDb.go#L62))
- [ ] Use DynamoDB un/marshalling for millisecond resolution checkpoints instead of storing date/time strings (see [pipeline-tools](https://github.com/3box/pipeline-tools/blob/develop/cd/manager/aws/dynamoDb.go#L305)
