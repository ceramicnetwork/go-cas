package db

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/google/uuid"

	"github.com/jackc/pgx/v5"

	"github.com/ceramicnetwork/go-cas/models"
)

type AnchorDatabase struct {
	opts AnchorDbOpts
}

type anchorRequest struct {
	Id        uuid.UUID
	Cid       string
	StreamId  string
	CreatedAt time.Time
	Timestamp time.Time
	Metadata  *models.StreamMetadata
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

func (adb *AnchorDatabase) GetRequests(status models.RequestStatus, since time.Time, limit int) ([]*models.AnchorRequestMessage, error) {
	query := "SELECT REQ.id, REQ.cid, REQ.stream_id, REQ.created_at, REQ.timestamp, META.metadata FROM request AS REQ LEFT JOIN metadata AS META USING (stream_id) WHERE status = $1 AND REQ.created_at > $2 ORDER BY REQ.created_at LIMIT $3"
	anchorRequests, err := adb.query(
		query,
		status,
		since.Format(models.DbDateFormat),
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
				Timestamp: anchorReq.Timestamp,
				Metadata:  anchorReq.Metadata,
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
		err = rows.Scan(
			&anchorReq.Id,
			&anchorReq.Cid,
			&anchorReq.StreamId,
			&anchorReq.CreatedAt,
			&anchorReq.Timestamp,
			&anchorReq.Metadata,
		)
		if err != nil {
			log.Printf("query: error scanning db row: %v", err)
			return nil, err
		}
		anchorRequests = append(anchorRequests, anchorReq)
	}
	return anchorRequests, nil
}

func (adb *AnchorDatabase) UpdateStatus(id uuid.UUID, status models.RequestStatus, message string) error {
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
		log.Printf("update: error connecting to db: %v", err)
		return err
	}
	defer conn.Close(context.Background())

	_, err = conn.Exec(
		context.Background(), "UPDATE request SET status = $1, message = $2, updated_at = $3 WHERE id = $4",
		status,
		message,
		time.Now().UTC(),
		id,
	)
	if err != nil {
		log.Printf("update: error updating db: %v", err)
		return err
	}
	return nil
}
