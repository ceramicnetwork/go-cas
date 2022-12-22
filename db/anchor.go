package db

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/google/uuid"

	"github.com/jackc/pgx/v5"

	"github.com/smrz2001/go-cas/models"
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
	opts AnchorDbOpts
}

type AnchorRequest struct {
	Id        uuid.UUID
	StreamId  string
	Cid       string
	CreatedAt time.Time
	Status    RequestStatus
	Message   string
	UpdatedAt time.Time
	Pinned    bool
}

type AnchorDbOpts struct {
	Host     string
	Port     string
	User     string
	Password string
	Name     string
}

func NewAnchorDb(opts AnchorDbOpts) *AnchorDatabase {
	return &AnchorDatabase{opts}
}

func (adb AnchorDatabase) PollRequests(checkpoint time.Time, limit int) ([]*models.AnchorRequest, error) {
	anchorRequests, err := adb.Query(
		"SELECT * FROM request WHERE status = $1 AND created_at > $2 ORDER BY created_at LIMIT $3",
		RequestStatus_Pending,
		checkpoint.Format(models.DbDateFormat),
		limit,
	)
	if err != nil {
		return nil, err
	} else if len(anchorRequests) > 0 {
		anchorReqMsgs := make([]*models.AnchorRequest, len(anchorRequests))
		for idx, anchorRequest := range anchorRequests {
			anchorReqMsgs[idx] = &models.AnchorRequest{
				Id:        anchorRequest.Id,
				StreamId:  anchorRequest.StreamId,
				Cid:       anchorRequest.Cid,
				CreatedAt: anchorRequest.CreatedAt,
			}
		}
		return anchorReqMsgs, nil
	}
	return nil, nil
}

func (adb AnchorDatabase) Query(sql string, args ...any) ([]*AnchorRequest, error) {
	dbCtx, dbCancel := context.WithTimeout(context.Background(), models.DefaultHttpWaitTime)
	defer dbCancel()

	connUrl := fmt.Sprintf(
		"postgres://%s:%s@%s:%s/%s",
		adb.opts.User,
		adb.opts.Password,
		adb.opts.Host,
		adb.opts.Port,
		adb.opts.Name,
	)
	conn, err := pgx.Connect(dbCtx, connUrl)
	if err != nil {
		log.Printf("query: error connecting to db: %v", err)
		return nil, err
	}
	defer conn.Close(context.Background())

	rows, err := conn.Query(context.Background(), sql, args...)
	if err != nil {
		log.Printf("query: error querying db: %v", err)
		return nil, err
	}
	defer rows.Close()

	anchorRequests := make([]*AnchorRequest, 0)
	for rows.Next() {
		anchorRequest := new(AnchorRequest)
		_ = rows.Scan(
			&anchorRequest.Id,
			&anchorRequest.Status,
			&anchorRequest.Cid,
			&anchorRequest.StreamId,
			&anchorRequest.Message,
			&anchorRequest.CreatedAt,
			&anchorRequest.UpdatedAt,
			&anchorRequest.Pinned,
		)
		anchorRequests = append(anchorRequests, anchorRequest)
	}
	if len(anchorRequests) > 0 {
		log.Printf("query: found %d requests, start=%s, end=%s", len(anchorRequests), anchorRequests[0].CreatedAt, anchorRequests[len(anchorRequests)-1].CreatedAt)
	}
	return anchorRequests, nil
}
