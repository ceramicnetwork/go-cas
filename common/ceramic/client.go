package ceramic

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io/ioutil"
	"log"
	"net/http"
	"sync/atomic"

	"github.com/multiformats/go-multibase"
	"github.com/multiformats/go-varint"

	"github.com/ipfs/go-cid"

	"github.com/abevier/tsk/batch"
	"github.com/abevier/tsk/ratelimiter"
	"github.com/abevier/tsk/results"

	"github.com/smrz2001/go-cas/models"
)

type innerClient struct {
	url          string
	queryLimiter *ratelimiter.RateLimiter[*models.CeramicQuery, *models.StreamState]
	pinLimiter   *ratelimiter.RateLimiter[*models.CeramicPin, *models.CeramicPinResult]
	mqBatcher    *batch.Executor[*models.CeramicQuery, *models.StreamState]
}

type Client struct {
	clients  []innerClient
	pinCtr   int64
	queryCtr int64
}

func NewCeramicClient(urls []string) *Client {
	beOpts := batch.Opts{MaxSize: models.DefaultBatchMaxDepth, MaxLinger: models.DefaultBatchMaxLinger}
	rlOpts := ratelimiter.Opts{
		Limit:             models.DefaultRateLimit,
		Burst:             models.DefaultRateLimit,
		MaxQueueDepth:     models.DefaultQueueDepthLimit,
		FullQueueStrategy: ratelimiter.BlockWhenFull,
	}
	client := &Client{pinCtr: -1, queryCtr: -1}
	clients := make([]innerClient, len(urls))
	queryLimiterGen := func(idx int) *ratelimiter.RateLimiter[*models.CeramicQuery, *models.StreamState] {
		return ratelimiter.New(rlOpts, func(ctx context.Context, query *models.CeramicQuery) (*models.StreamState, error) {
			// Use the presence or absence of the genesis CID to decide whether to perform a Ceramic stream query, or a
			// Ceramic multiquery for a missing commit.
			if (query.GenesisCid == nil) || (len(*query.GenesisCid) == 0) {
				return clients[idx].doQuery(ctx, query.StreamId)
			}
			return clients[idx].mqBatcher.Submit(ctx, query)
		})
	}
	pinLimiterGen := func(idx int) *ratelimiter.RateLimiter[*models.CeramicPin, *models.CeramicPinResult] {
		return ratelimiter.New(rlOpts, func(ctx context.Context, pin *models.CeramicPin) (*models.CeramicPinResult, error) {
			return clients[idx].doPin(ctx, pin.StreamId)
		})
	}
	// TODO: Use better context
	mqBatcherGen := func(idx int) *batch.Executor[*models.CeramicQuery, *models.StreamState] {
		return batch.New[*models.CeramicQuery, *models.StreamState](beOpts, func(queries []*models.CeramicQuery) ([]results.Result[*models.StreamState], error) {
			return clients[idx].multiquery(context.Background(), queries)
		})
	}
	for i := 0; i < len(urls); i++ {
		clients[i] = innerClient{
			urls[i],
			queryLimiterGen(i),
			pinLimiterGen(i),
			mqBatcherGen(i),
		}
	}
	client.clients = clients
	return client
}

func (c *Client) Query(ctx context.Context, query *models.CeramicQuery) (*models.StreamState, error) {
	ctr := atomic.AddInt64(&c.queryCtr, 1)
	return c.clients[ctr%int64(len(c.clients))].queryLimiter.Submit(ctx, query)
}

func (c *Client) Pin(ctx context.Context, pin *models.CeramicPin) error {
	ctr := atomic.AddInt64(&c.pinCtr, 1)
	_, err := c.clients[ctr%int64(len(c.clients))].pinLimiter.Submit(ctx, pin)
	return err
}

func (i *innerClient) multiquery(ctx context.Context, queries []*models.CeramicQuery) ([]results.Result[*models.StreamState], error) {
	queryResults := make([]results.Result[*models.StreamState], len(queries))
	if mqResp, err := i.doMultiquery(ctx, queries); err != nil {
		return nil, err
	} else {
		// Fan the multiquery results back out
		for idx, query := range queries {
			if streamState, found := mqResp[multiqueryId(query)]; found {
				streamState.Id = query.StreamId
				queryResults[idx] = results.New[*models.StreamState](streamState, nil)
			} else {
				queryResults[idx] = results.New[*models.StreamState](nil, nil)
			}
		}
	}
	return queryResults, nil
}

func (i *innerClient) doPin(ctx context.Context, streamId string) (*models.CeramicPinResult, error) {
	log.Printf("pin: %s", streamId)

	pCtx, pCancel := context.WithTimeout(ctx, models.CeramicPinTimeout)
	defer pCancel()

	req, err := http.NewRequestWithContext(pCtx, "POST", i.url+"/api/v0/pins/"+streamId, nil)
	if err != nil {
		log.Printf("pin: error creating request: %v", err)
		return nil, err
	}
	req.Header.Add("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		log.Printf("pin: error submitting request: %v", err)
		return nil, err
	}
	defer resp.Body.Close()
	respBody, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		log.Printf("pin: error reading response: %v", err)
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		log.Printf("pin: error in response: %v", resp.StatusCode)
		return nil, errors.New("pin: error in response")
	}
	pResp := new(models.CeramicPinResult)
	if err = json.Unmarshal(respBody, pResp); err != nil {
		log.Printf("pin: error unmarshaling response: %v", err)
		return nil, err
	}
	log.Printf("pin: resp%+v", *pResp)
	return pResp, nil
}

func (i *innerClient) doQuery(ctx context.Context, streamId string) (*models.StreamState, error) {
	log.Printf("query: %s", streamId)

	qCtx, qCancel := context.WithTimeout(ctx, models.CeramicStreamLoadTimeout)
	defer qCancel()

	req, err := http.NewRequestWithContext(qCtx, "GET", i.url+"/api/v0/streams/"+streamId, nil)
	if err != nil {
		log.Printf("query: error creating stream request: %v", err)
		return nil, err
	}
	req.Header.Add("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		log.Printf("query: error submitting stream request: %v", err)
		return nil, err
	}
	defer resp.Body.Close()
	respBody, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		log.Printf("query: error reading stream response: %v", err)
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		log.Printf("error in query: %v, %s", resp.StatusCode, respBody)
		return nil, errors.New("query: error in response")
	}
	stream := models.Stream{}
	if err = json.Unmarshal(respBody, &stream); err != nil {
		log.Printf("query: error unmarshaling response: %v", err)
		return nil, err
	}
	stream.State.Id = streamId
	log.Printf("query: resp=%+v", stream)
	return &stream.State, nil
}

func (i *innerClient) doMultiquery(mqCtx context.Context, queries []*models.CeramicQuery) (map[string]*models.StreamState, error) {
	type streamQuery struct {
		StreamId string `json:"streamId"`
	}
	type multiquery struct {
		Queries []*streamQuery `json:"queries"`
	}
	mq := multiquery{make([]*streamQuery, len(queries))}
	for idx, query := range queries {
		mq.Queries[idx] = &streamQuery{multiqueryId(query)}
	}
	mqBody, err := json.Marshal(mq)
	if err != nil {
		log.Printf("multiquery: error creating request json: %v", err)
		return nil, err
	}
	log.Printf("multiquery: %s", mqBody)

	mqCtx, cancel := context.WithTimeout(context.Background(), models.CeramicMultiqueryTimeout)
	defer cancel()

	req, err := http.NewRequestWithContext(mqCtx, "POST", i.url+"/api/v0/multiqueries", bytes.NewBuffer(mqBody))
	if err != nil {
		log.Printf("multiquery: error creating request: %v", err)
		return nil, err
	}
	req.Header.Add("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		log.Printf("multiquery: error submitting request: %v", err)
		return nil, err
	}
	defer resp.Body.Close()
	respBody, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		log.Printf("multiquery: error reading response: %v", err)
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		log.Printf("multiquery: error in response: %v", resp.StatusCode)
		return nil, errors.New("multiquery: error in response")
	}
	mqResp := make(map[string]*models.StreamState)
	if err = json.Unmarshal(respBody, &mqResp); err != nil {
		log.Printf("multiquery: error unmarshaling response: %v", err)
		return nil, err
	}
	log.Printf("multiquery: streams=%d, resp=%+v", len(mqResp), mqResp)
	return mqResp, nil
}

func multiqueryId(query *models.CeramicQuery) string {
	buf := bytes.Buffer{}
	mqId := ""
	// If the genesis CID is present, we're trying to find a missing CID, otherwise we're doing a plain stream query.
	if query.GenesisCid != nil {
		buf.Write(varint.ToUvarint(206))
		buf.Write(varint.ToUvarint(uint64(*query.StreamType)))
		genesisCid, _ := cid.Parse(query.GenesisCid)
		commitCid, _ := cid.Parse(query.Cid)
		buf.Write(genesisCid.Bytes())
		buf.Write(commitCid.Bytes())
		mqId, _ = multibase.Encode(multibase.Base36, buf.Bytes())
	} else {
		mqId = query.StreamId
	}
	return mqId
}
