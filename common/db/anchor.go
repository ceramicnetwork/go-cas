package db

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/google/uuid"

	"github.com/jackc/pgx/v5"

	"github.com/ceramicnetwork/go-cas/common"
	"github.com/ceramicnetwork/go-cas/models"
)

type AnchorDatabase struct {
	opts   anchorDbOpts
	logger models.Logger
}

type anchorDbOpts struct {
	Host     string
	Port     string
	User     string
	Password string
	Name     string
}

func NewAnchorDb(logger models.Logger) *AnchorDatabase {
	return &AnchorDatabase{anchorDbOpts{
		Host:     os.Getenv(common.Env_PgHost),
		Port:     os.Getenv(common.Env_PgPort),
		User:     os.Getenv(common.Env_PgUser),
		Password: os.Getenv(common.Env_PgPassword),
		Name:     os.Getenv(common.Env_PgDb),
	}, logger}
}

func (adb *AnchorDatabase) GetRequests(ctx context.Context, status models.RequestStatus, newerThan time.Time, olderThan time.Time, limit int) ([]*models.AnchorRequest, error) {
	query := "SELECT REQ.id, REQ.cid, REQ.stream_id, REQ.origin, REQ.timestamp, REQ.created_at, META.metadata FROM request AS REQ LEFT JOIN metadata AS META USING (stream_id) WHERE status = $1 AND REQ.created_at > $2 AND created_at < $3 ORDER BY REQ.created_at LIMIT $4"
	return adb.query(
		ctx,
		query,
		status,
		newerThan.Format(common.DbDateFormat),
		olderThan.Format(common.DbDateFormat),
		limit,
	)
}

func (adb *AnchorDatabase) query(ctx context.Context, sql string, args ...any) ([]*models.AnchorRequest, error) {
	dbCtx, dbCancel := context.WithTimeout(ctx, common.DefaultRpcWaitTime)
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
		adb.logger.Errorf("error connecting to db: %v", err)
		return nil, err
	}
	defer conn.Close(dbCtx)

	rows, err := conn.Query(dbCtx, sql, args...)
	if err != nil {
		adb.logger.Errorf("error querying db: %v", err)
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
			adb.logger.Errorf("error scanning db row: %v", err)
			return nil, err
		}
		anchorRequests = append(anchorRequests, anchorReq)
	}
	return anchorRequests, nil
}

func (adb *AnchorDatabase) UpdateStatus(ctx context.Context, id uuid.UUID, status models.RequestStatus, allowedSourceStatuses []models.RequestStatus) error {
	dbCtx, dbCancel := context.WithTimeout(ctx, common.DefaultRpcWaitTime)
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
		adb.logger.Errorf("error connecting to db: %v", err)
		return err
	}
	defer conn.Close(dbCtx)

	condition := ""
	if len(allowedSourceStatuses) > 0 {
		condition = " AND (status = " + strconv.Itoa(int(allowedSourceStatuses[0]))
		for i := 1; i < len(allowedSourceStatuses); i++ {
			condition = fmt.Sprintf("%s OR status = %s", condition, strconv.Itoa(int(allowedSourceStatuses[i])))
		}
		condition += ")"
	}
	_, err = conn.Exec(
		dbCtx,
		"UPDATE request SET status = $1, updated_at = $2 WHERE id = $3"+condition,
		status,
		time.Now().UTC(),
		id,
	)
	if err != nil {
		adb.logger.Errorf("error updating db: %v", err)
		return err
	}
	return nil
}
