package main

import (
	"log"
	"sync"

	"github.com/joho/godotenv"

	"github.com/smrz2001/go-cas/services/batch"
	"github.com/smrz2001/go-cas/services/load"
	"github.com/smrz2001/go-cas/services/poll"
)

func main() {
	if err := godotenv.Load("env/.env"); err != nil {
		log.Fatal("Error loading .env file")
	}
	log.SetFlags(log.Ldate | log.Ltime | log.Lshortfile)

	wg := sync.WaitGroup{}
	// Set this to the number of services being invoked below
	wg.Add(3)

	// 1. Poll service
	//  - Poll Postgres for new anchor requests, which avoids changes to the existing CAS API service.
	//  - Post request to Request queue.
	//  - Write polling checkpoint to state DB.
	go poll.NewPollingService().Poll()

	// 2. Stream loading service
	//  - Read requests from the Request queue.
	//  - Send one or more multiqueries to Ceramic with stream/CID load requests.
	//  - Write successful results to DB and post to Ready queue.
	go load.NewLoadingService().Load()

	// 3. Batching service
	//  - Read requests from Ready queue and add streams cache.
	//  - For every request (and on some interval), check:
	//    - If oldest entry in cache is older than batch expiration time (5 minutes).
	//    - If number of streams in cache is equal to maximum batch size (1024).
	//  - If yes, post job to Worker queue with batch.
	go batch.NewBatchingService().Batch()

	wg.Wait()
}
