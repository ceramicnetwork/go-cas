# Go Ceramic Anchor Service

## TODOs (roughly ordered by priority)

### Major

- [ ] Use futures for Ceramic loading - stream query to load CIDs, multiquery to load missing CIDs <- can use batcher to do multiqueries so we can aggregate calls across multiple missing CIDs
- [ ] Hide tq/be in Commit loader <- initial version is Ceramic loading, changes to IPFS loading after Cw/oC Ph1, then just goes away once we start getting CAR files
- [ ] Implement batcher running cache
- [ ] Implement SQS FIFO queues (`Request`, `Ready`, `Worker`, and `Failure` queues)
- [ ] Use BE to consolidate SQS calls <- since we're charged per API call
- [ ] Use [go-sqs](https://github.com/ABevier/go-sqs) for more sophisticated/systematic SQS processing/scaling
- [ ] Implement failure handler <- _should_ fail requests in Postgres, for now, can just fail them in DynamoDB. There could be other failures to so will need to handle them here.
- [ ] Add DynamoDB table creation (see code in [pipeline-tools](https://github.com/3box/pipeline-tools/blob/develop/cd/manager/aws/dynamoDb.go#L62))
- [ ] DLQ?
- [ ] rainy day
- [ ] deployments?
- [ ] CI CD
- [ ] scaling
- [ ] Unit tests (backward compatibility tests?)
- [ ] geo-redundancy for DynamoDB/SQS?
- [ ] metrics

### Minor

- [ ] ?

### Starter

- [ ] ?
