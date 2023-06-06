# Go Ceramic Anchor Service

## Architecture

### Data Pipeline

https://lucid.app/documents/view/c9b677a7-2b60-4bec-89c7-43d10e2a262e

### Flows

https://lucid.app/documents/view/9fb65517-add6-48f5-a35b-e8c5835a9762

## TODOs (roughly ordered by priority)

### Major

- [ ] Metrics
- [ ] Performance tests
- [ ] Clean up logging in `go-sqs` and `go-cas`
- [ ] Graceful shutdown
- [ ] Prepare Anchor Worker code to post anchor results to a queue

### Minor

- [ ] Clarify contexts being used in various spots - operation ctx vs. server ctx

### Development

You can run integration tests through docker via the following command:

```
docker-compose -f ./docker-compose.yml -f docker-compose.integration.yml up -d
```

This will bring up all the necessary containers to have the tests run.
