package db

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/smrz2001/go-cas/models"
)

type AnchorDatabase struct {
	host     string
	port     string
	user     string
	password string
	db       string
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

func (adb AnchorDatabase) Poll(checkpoint time.Time, limit int) ([]*models.AnchorRequest, error) {
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
		"select * from request where status = $1 and created_at > $2 LIMIT $3",
		models.RequestStatus_Completed,
		checkpoint.Format(models.DbDateFormat),
		limit,
	)
	if err != nil {
		log.Printf("anchorDb: error querying db: %v", err)
		return nil, err
	}
	defer rows.Close()

	anchorReqs := make([]*models.AnchorRequest, 0)
	anchorReq := models.AnchorRequest{}
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
		anchorReqs = append(anchorReqs, &anchorReq)
	}
	return anchorReqs, nil
}
