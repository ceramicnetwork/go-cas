package db

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/google/uuid"

	"github.com/jackc/pgx/v5"

	"github.com/smrz2001/go-cas/models"
	"github.com/smrz2001/go-cas/queue/messages"
)

type RequestStatus uint8

const (
	RequestStatus_Pending RequestStatus = iota
	RequestStatus_Processing
	RequestStatus_Completed
	RequestStatus_Failed
	RequestStatus_Ready
)

type AnchorDatabase struct {
	host     string
	port     string
	user     string
	password string
	db       string
}

type anchorRequest struct {
	Id        uuid.UUID
	StreamId  string
	Cid       string
	CreatedAt time.Time
	Status    int
	Message   string
	UpdatedAt time.Time
	Pinned    bool
}

func NewAnchorDb() *AnchorDatabase {
	return &AnchorDatabase{
		host:     os.Getenv("PG_HOST"),
		port:     os.Getenv("PG_PORT"),
		user:     os.Getenv("PG_USER"),
		password: os.Getenv("PG_PASSWORD"),
		db:       os.Getenv("PG_DB"),
	}
}

func (adb AnchorDatabase) Poll(checkpoint time.Time, limit int) ([]*messages.AnchorRequest, error) {
	dbCtx, dbCancel := context.WithTimeout(context.Background(), models.DefaultHttpWaitTime)
	defer dbCancel()

	connUrl := fmt.Sprintf("postgres://%s:%s@%s:%s/%s", adb.user, adb.password, adb.host, adb.port, adb.db)
	conn, err := pgx.Connect(dbCtx, connUrl)
	if err != nil {
		log.Printf("anchorDb: error connecting to db: %v", err)
		return nil, err
	}
	defer conn.Close(context.Background())

	rows, err := conn.Query(
		context.Background(),
		"SELECT * FROM request WHERE status = $1 AND created_at > $2 ORDER BY created_at LIMIT $3",
		RequestStatus_Pending,
		checkpoint.Format(models.DbDateFormat),
		limit,
	)
	if err != nil {
		log.Printf("anchorDb: error querying db: %v", err)
		return nil, err
	}
	defer rows.Close()

	var anchorReqs []*messages.AnchorRequest
	anchorReq := anchorRequest{}
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
		anchorReqs = append(anchorReqs, &messages.AnchorRequest{
			Id:        anchorReq.Id,
			StreamId:  anchorReq.StreamId,
			Cid:       anchorReq.Cid,
			CreatedAt: anchorReq.CreatedAt,
		})
	}
	if len(anchorReqs) > 0 {
		log.Printf("anchorDb: found %d requests, start=%s, end=%s", len(anchorReqs), anchorReqs[0].CreatedAt, anchorReqs[len(anchorReqs)-1].CreatedAt)
	}
	return anchorReqs, nil
}
