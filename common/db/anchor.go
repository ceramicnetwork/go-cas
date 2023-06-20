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

func (adb *AnchorDatabase) GetRequests(ctx context.Context, status models.RequestStatus, since time.Time, limit int) ([]*models.AnchorRequest, error) {
	query := "SELECT REQ.id, REQ.cid, REQ.stream_id, REQ.origin, REQ.timestamp, REQ.created_at, META.metadata FROM request AS REQ LEFT JOIN metadata AS META USING (stream_id) WHERE status = $1 AND REQ.created_at > $2 ORDER BY REQ.created_at LIMIT $3"
	return adb.query(ctx, query, status, since.Format(models.DbDateFormat), limit)
}

func (adb *AnchorDatabase) query(ctx context.Context, sql string, args ...any) ([]*models.AnchorRequest, error) {
	dbCtx, dbCancel := context.WithTimeout(ctx, models.DefaultDbWaitTime)
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
	defer conn.Close(dbCtx)

	rows, err := conn.Query(dbCtx, sql, args...)
	if err != nil {
		log.Printf("query: error querying db: %v", err)
		return nil, err
	}
	defer rows.Close()

	anchorRequests := make([]*models.AnchorRequest, 0)
	for rows.Next() {
		anchorReq := new(models.AnchorRequest)
		err = rows.Scan(
			&anchorReq.Id,
			&anchorReq.Cid,
			&anchorReq.StreamId,
			&anchorReq.Origin,
			&anchorReq.Timestamp,
			&anchorReq.CreatedAt,
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

func (adb *AnchorDatabase) UpdateStatus(ctx context.Context, id uuid.UUID, status models.RequestStatus, allowedSourceStatuses []models.RequestStatus) error {
	dbCtx, dbCancel := context.WithTimeout(ctx, models.DefaultDbWaitTime)
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
	defer conn.Close(dbCtx)

	condition := ""
	if len(allowedSourceStatuses) > 0 {
		condition = " AND (status = " + string(allowedSourceStatuses[0])
		for i := 1; i < len(allowedSourceStatuses); i++ {
			condition = fmt.Sprintf("%s OR status = %s", condition, string(allowedSourceStatuses[i]))
		}
		condition += ")"
	}
	_, err = conn.Exec(
		dbCtx,
		"UPDATE request SET status = $1, updated_at = $3 WHERE id = $4"+condition,
		status,
		time.Now().UTC(),
		id,
	)
	if err != nil {
		log.Printf("update: error updating db: %v", err)
		return err
	}
	return nil
}
