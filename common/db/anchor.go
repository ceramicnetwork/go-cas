package db

import (
	"context"
	"fmt"
	"log"
	"regexp"
	"strconv"
	"time"

	"github.com/google/uuid"

	"github.com/jackc/pgx/v5"

	"github.com/smrz2001/go-cas/models"
)

type AnchorDatabase struct {
	opts     AnchorDbOpts
	reloadRe *regexp.Regexp
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
	return &AnchorDatabase{
		opts,
		regexp.MustCompile(`^Reload attempt #[0-9] times\.$`),
	}
}

func (adb *AnchorDatabase) GetRequests(status models.RequestStatus, newerThan time.Time, olderThan time.Time, msgFilters []string, limit int) ([]*models.AnchorRequestMessage, error) {
	query := "SELECT * FROM request WHERE status = $1 AND updated_at > $2 AND updated_at < $3"
	if len(msgFilters) > 0 {
		for _, msgFilter := range msgFilters {
			query += " AND message NOT LIKE '%" + msgFilter + "%'"
		}
	}
	anchorRequests, err := adb.query(
		query+" ORDER BY updated_at LIMIT $4",
		status,
		newerThan.Format(models.DbDateFormat),
		olderThan.Format(models.DbDateFormat),
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
				UpdatedAt: anchorReq.UpdatedAt,
				Attempt:   adb.findAttemptNum(anchorReq.Message),
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
			&anchorReq.Pinned,
			&anchorReq.CreatedAt,
			&anchorReq.UpdatedAt,
			&anchorReq.Cid,
			&anchorReq.StreamId,
			&anchorReq.Message,
		)
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

func (adb *AnchorDatabase) findAttemptNum(message string) *int {
	attemptStr := adb.reloadRe.FindString(message)
	var attempt *int = nil
	if len(attemptStr) > 0 {
		if att, err := strconv.Atoi(attemptStr); err == nil {
			attempt = &att
		}
	}
	return attempt
}
