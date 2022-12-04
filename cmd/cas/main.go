package main

import (
	"log"
	"sync"

	"github.com/joho/godotenv"

	"github.com/smrz2001/go-cas/services/batch"
	"github.com/smrz2001/go-cas/services/failure"
	"github.com/smrz2001/go-cas/services/poll"
	"github.com/smrz2001/go-cas/services/stream"
)

func main() {
	if err := godotenv.Load("env/.env"); err != nil {
		log.Fatal("Error loading .env file")
	}
	log.SetFlags(log.Ldate | log.Ltime | log.Lshortfile)

	wg := sync.WaitGroup{}
	wg.Add(4)

	// 1. Poll service
	//  - Poll Postgres for new anchor requests, which avoids changes to the existing CAS API service.
	//  - Post request to Request queue.
	//  - Write requests and updated Postgres polling checkpoint to DB.
	go poll.LoadRequestsFromDb()

	// 2. Stream loading service
	//  - Read requests from the Request queue.
	//  - Send one or more multi-queries to Ceramic with corresponding stream load requests.
	//  - Wait for multi-query results:
	//    - Write successful results to DB and post to Ready queue.
	//    - TODO: Post failures to Failure queue
	go stream.LoadStreamsFromCeramic()

	// 3. Batching service
	//  - Read requests from Ready queue and add tips to cache.
	//  - For every request (and on some interval), check:
	//    - If oldest entry in cache is older than batch expiration time (5 minutes).
	//    - If number of streams in cache is equal to maximum batch size (1024).
	//  - If yes, post job to Worker queue with batch.
	go batch.CollectAnchorBatch()

	// 4. TODO: Failure service
	go failure.HandleFailures()

	wg.Wait()
}
