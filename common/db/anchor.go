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

type AnchorDatabase struct {
	opts AnchorDbOpts
}

type anchorRequest struct {
	Id        uuid.UUID
	StreamId  string
	Cid       string
	CreatedAt time.Time
	Status    models.RequestStatus
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

func (adb *AnchorDatabase) RequestsSinceCheckpoint(checkpoint time.Time, limit int) ([]*models.AnchorRequestMessage, error) {
	anchorRequests, err := adb.query(
		"SELECT * FROM request WHERE status = $1 AND created_at > $2 ORDER BY created_at LIMIT $3",
		models.RequestStatus_Pending,
		checkpoint.Format(models.DbDateFormat),
		limit,
	)
	if err != nil {
		return nil, err
	} else if len(anchorRequests) > 0 {
		anchorReqMsgs := make([]*models.AnchorRequestMessage, len(anchorRequests))
		for idx, anchorReq := range anchorRequests {
			anchorReqMsgs[idx] = &models.AnchorRequestMessage{
				Id:        anchorReq.Id,
				StreamId:  anchorReq.StreamId,
				Cid:       anchorReq.Cid,
				CreatedAt: anchorReq.CreatedAt,
			}
		}
		return anchorReqMsgs, nil
	}
	return nil, nil
}

func (adb *AnchorDatabase) query(sql string, args ...any) ([]*anchorRequest, error) {
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

	anchorRequests := make([]*anchorRequest, 0)
	for rows.Next() {
		anchorReq := new(anchorRequest)
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
		anchorRequests = append(anchorRequests, anchorReq)
	}
	return anchorRequests, nil
}
