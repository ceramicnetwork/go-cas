package poll

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/smrz2001/go-cas"
	"github.com/smrz2001/go-cas/aws"
	"github.com/smrz2001/go-cas/services/stream"
)

var RowCtr = 0

func LoadRequestsFromDb() {
	cfg, err := aws.Config()
	if err != nil {
		log.Fatalf("failed to create aws cfg: %v", err)
	}
	db := aws.NewDynamoDb(cfg)

	// Read polling checkpoint from DB
	checkpoint, err := db.GetCheckpoint(cas.CheckpointType_Poll)
	if err != nil {
		log.Fatalf("error querying checkpoint: %v", err)
	}
	log.Printf("db checkpoint: %s", checkpoint)

	// TODO: Graceful shutdown
	for {
		if newCheckpoint, err := loadFromPostgres(db, checkpoint); err != nil {
			log.Printf("error loading from postgres: %v", err)
		} else {
			tmpCheckpoint := newCheckpoint.Format(cas.DbDateFormat)
			if tmpCheckpoint != checkpoint {
				if _, err = db.UpdateCheckpoint(cas.CheckpointType_Poll, tmpCheckpoint); err != nil {
					log.Printf("error updating checkpoint: %v", err)
				} else {
					// Only update checkpoint in-memory once it's been written to DB
					checkpoint = tmpCheckpoint
				}
			}
		}
		// Sleep even if we had errors so that we don't get stuck in a tight loop
		time.Sleep(cas.DefaultTick)
	}
}

func loadFromPostgres(db cas.Database, checkpoint string) (time.Time, error) {
	dbCtx, dbCancel := context.WithTimeout(context.Background(), cas.DefaultHttpWaitTime)
	defer dbCancel()

	// Load starting from checkpoint
	pgHost := os.Getenv("PG_HOST")
	pgPort := os.Getenv("PG_PORT")
	pgUser := os.Getenv("PG_USER")
	pgPassword := os.Getenv("PG_PASSWORD")
	pgDb := os.Getenv("PG_DB")
	connUrl := fmt.Sprintf("postgres://%s:%s@%s:%s/%s", pgUser, pgPassword, pgHost, pgPort, pgDb)
	conn, err := pgx.Connect(dbCtx, connUrl)
	if err != nil {
		log.Printf("error connecting to db: %v", err)
		return time.Time{}, err
	}
	defer conn.Close(context.Background())

	rows, err := conn.Query(
		context.Background(),
		"select * from request where status = $1 and created_at > $2 LIMIT $3",
		cas.RequestStatus_Pending,
		checkpoint,
		cas.DbLoadLimit,
	)
	if err != nil {
		log.Printf("error querying db: %v", err)
		return time.Time{}, err
	}
	defer rows.Close()

	anchorReq := cas.AnchorRequest{}
	newCheckpoint, _ := time.Parse(cas.DbDateFormat, checkpoint)
	for rows.Next() {
		_ = rows.Scan(
			&anchorReq.Id,
			&anchorReq.Status,
			&anchorReq.Cid,
			&anchorReq.StreamId,
			&anchorReq.Message,
			&anchorReq.CreatedAt,
			&anchorReq.UpdatedAt,
			&anchorReq.Pinned,
		)

		// Store in DB first
		streamCid := cas.StreamCid{
			Id:  anchorReq.StreamId,
			Cid: anchorReq.Cid,
		}
		// This will not overwrite existing stream/CID entries
		if existing, err := db.CreateCid(&streamCid); err != nil {
			log.Printf("error writing to db: %v", err)
			return time.Time{}, err
		} else if !existing {
			// Post to Request queue only if request was written to DB, i.e. if it didn't already exist.
			stream.RequestCh <- streamCid
			newCheckpoint = anchorReq.CreatedAt
		} else {
			log.Printf("stream=%s, cid=%s already present in db", anchorReq.StreamId, anchorReq.Cid)
		}
		// Sleep for 1 second every 5 writes so we don't bust our throughput. TODO: Consider using the batch executor here.
		RowCtr++
		if (RowCtr % 5) == 0 {
			time.Sleep(time.Second)
		}
		log.Printf("processed %d rows: %v", RowCtr, anchorReq)
	}
	return newCheckpoint, nil
}
