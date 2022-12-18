# Go Ceramic Anchor Service

## Architecture

### Data Pipeline

https://lucid.app/documents/view/c9b677a7-2b60-4bec-89c7-43d10e2a262e

### Flows

https://lucid.app/documents/view/9fb65517-add6-48f5-a35b-e8c5835a9762

## TODOs (roughly ordered by priority)

### Major

- [ ] Implement batcher running cache
- [ ] Implement `Worker` and `Failure` queues
- [ ] Implement failure handler <- _should_ fail requests in Postgres, for now, can just fail them in DynamoDB. There could be other failures to so will need to handle them here.
- [ ] DLQ?
- [ ] Use [go-sqs](https://github.com/ABevier/go-sqs) for more sophisticated/systematic SQS processing/scaling
- [ ] Add DynamoDB table creation (see code in [pipeline-tools](https://github.com/3box/pipeline-tools/blob/develop/cd/manager/aws/dynamoDb.go#L62))
- [ ] CI/CD
- [ ] Unit tests (backward compatibility tests?)
- [ ] Performance tests
- [ ] Metrics

### Minor

- [ ] ?

### Starter

- [ ] ?
