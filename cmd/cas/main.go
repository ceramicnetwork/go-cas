package main

import (
	"log"
	"os"
	"sync"

	"github.com/joho/godotenv"

	"github.com/aws/aws-sdk-go-v2/service/sqs"

	"github.com/smrz2001/go-cas/common/ceramic"
	"github.com/smrz2001/go-cas/common/db"
	"github.com/smrz2001/go-cas/common/queue"
	"github.com/smrz2001/go-cas/common/utils"
	"github.com/smrz2001/go-cas/models"
	"github.com/smrz2001/go-cas/services"
)

func main() {
	if err := godotenv.Load("env/.env"); err != nil {
		log.Fatal("Error loading .env file")
	}
	log.SetFlags(log.Ldate | log.Ltime | log.Lshortfile)

	awsCfg, err := utils.AwsConfig()
	if err != nil {
		log.Fatalf("newCeramicLoader: error creating aws cfg: %v", err)
	}

	anchorDb := db.NewAnchorDb(db.AnchorDbOpts{
		Host:     os.Getenv("PG_HOST"),
		Port:     os.Getenv("PG_PORT"),
		User:     os.Getenv("PG_USER"),
		Password: os.Getenv("PG_PASSWORD"),
		Name:     os.Getenv("PG_DB"),
	})

	// Use override endpoint, if specified, for state DB so that we can store jobs locally, while hitting regular AWS
	// endpoints for other operations. This allows local testing without affecting live processes in AWS.
	dbAwsCfg := awsCfg
	stateDbEndpoint := os.Getenv("DB_AWS_ENDPOINT")
	if len(stateDbEndpoint) > 0 {
		log.Printf("using custom state db endpoint: %s", stateDbEndpoint)
		dbAwsCfg, err = utils.AwsConfigWithOverride(stateDbEndpoint)
		if err != nil {
			log.Fatalf("failed to create aws cfg: %v", err)
		}
	}
	stateDb := db.NewStateDb(dbAwsCfg)

	sqsClient := sqs.NewFromConfig(awsCfg)

	wg := sync.WaitGroup{}
	// Set this to the number of services being invoked below
	wg.Add(3)

	// 1. Poll service
	//  - Poll Postgres for new anchor requests, which avoids changes to the existing CAS API service.
	//  - Post request to Request queue.
	//  - Write polling checkpoint to state DB.
	go services.NewPollingService(
		anchorDb,
		stateDb,
		queue.NewPublisher(models.QueueType_Request, sqsClient),
	).Run()

	failurePublisher := queue.NewPublisher(models.QueueType_Failure, sqsClient)

	// 2. Stream loading service
	//  - Read requests from the Request queue.
	//  - Send one or more multiqueries to Ceramic with stream/CID load requests.
	//  - Write successful results to DB and post to Ready queue.
	loadingService := services.NewLoadingService(
		ceramic.NewCeramicClient(),
		queue.NewPublisher(models.QueueType_Multiquery, sqsClient),
		queue.NewPublisher(models.QueueType_Ready, sqsClient),
		failurePublisher,
		stateDb,
	)
	reqConsumer := queue.NewConsumer(models.QueueType_Request, sqsClient, loadingService.ProcessQuery)
	mqConsumer := queue.NewConsumer(models.QueueType_Multiquery, sqsClient, loadingService.ProcessQuery)
	// Start the two queue consumers. These consumers will be responsible for scaling event processing up based on load,
	// and also maintaining backpressure on the queues.
	mqConsumer.Start()
	reqConsumer.Start()

	// 3. Batching service
	//  - Read requests from Ready queue and add streams cache.
	//  - For every request (and on some interval), check:
	//    - If oldest entry in cache is older than batch expiration time (e.g. 5 minutes)
	//    - If number of streams in cache is equal to maximum batch size (1024)
	//  - If yes, post job to Worker queue with batch.
	batchingService := services.NewBatchingService(
		queue.NewPublisher(models.QueueType_Worker, sqsClient),
		failurePublisher,
	)
	readyConsumer := queue.NewConsumer(models.QueueType_Ready, sqsClient, batchingService.ProcessReady)
	// Start the Ready queue consumer
	readyConsumer.Start()

	wg.Wait()
}
