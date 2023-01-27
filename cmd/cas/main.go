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

	// Flow:
	// ====
	// 1. Request polling service:
	//	- Poll anchor DB for new requests
	//  - Post requests to Pin queue
	// 2. Failure polling service:
	//	- Poll anchor DB for requests FAILED more than 6 hours ago
	//  - Post requests to the Load queue
	// 3. Pinning service:
	//	- Read requests from the Pin queue
	//  - Send stream pin requests to Ceramic
	// 4. Loading service:
	//	- Read requests from the Load queue
	//  - Send one or more multiqueries to Ceramic with stream/CID load requests
	//  - Write successful results to anchor DB

	// HTTP clients
	ceramicClient := ceramic.NewCeramicClient(os.Getenv("CERAMIC_URL"))
	sqsClient := sqs.NewFromConfig(awsCfg)

	// Queue publishers
	loadPublisher := queue.NewPublisher(models.QueueType_Load, sqsClient)
	pinPublisher := queue.NewPublisher(models.QueueType_Pin, sqsClient)
	statusPublisher := queue.NewPublisher(models.QueueType_Status, sqsClient)

	// Services
	pinningService := services.NewPinningService(ceramicClient)
	loadingService := services.NewLoadingService(ceramicClient, loadPublisher, statusPublisher, stateDb)
	statusService := services.NewStatusService(anchorDb)

	// Start the queue consumers. These consumers will be responsible for scaling event processing up based on load, and
	// also maintaining backpressure on the queues.
	queue.NewConsumer(pinPublisher, pinningService.Pin).Start()
	queue.NewConsumer(loadPublisher, loadingService.Load).Start()
	queue.NewConsumer(statusPublisher, statusService.Status).Start()

	// Start the polling services last
	wg := sync.WaitGroup{}
	wg.Add(2)
	// Poll from the anchor DB and post to the Pin queue for Ceramic to pin the corresponding streams
	go services.NewRequestPoller(anchorDb, stateDb, pinPublisher).Run()
	// Poll from the anchor DB and post to the Load queue for Ceramic to re-attempt loading the corresponding streams
	// and CIDs.
	go services.NewFailurePoller(anchorDb, stateDb, loadPublisher).Run()
	wg.Wait()
}
