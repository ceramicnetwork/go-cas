package ceramic

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io/ioutil"
	"log"
	"net/http"

	"github.com/multiformats/go-multibase"
	"github.com/multiformats/go-varint"

	"github.com/ipfs/go-cid"

	"github.com/smrz2001/go-cas/models"
)

type Client struct {
	url string
}

func NewCeramicClient(url string) *Client {
	return &Client{url}
}

func (c Client) Pin(ctx context.Context, streamId string) (*models.CeramicPinResult, error) {
	log.Printf("pin: %s", streamId)

	pCtx, pCancel := context.WithTimeout(ctx, models.CeramicPinTimeout)
	defer pCancel()

	req, err := http.NewRequestWithContext(pCtx, "POST", c.url+"/api/v0/pins/"+streamId, nil)
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

func (c Client) Query(ctx context.Context, streamId string) (*models.StreamState, error) {
	log.Printf("query: %s", streamId)

	qCtx, qCancel := context.WithTimeout(ctx, models.CeramicStreamLoadTimeout)
	defer qCancel()

	req, err := http.NewRequestWithContext(qCtx, "GET", c.url+"/api/v0/streams/"+streamId, nil)
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

func (c Client) Multiquery(mqCtx context.Context, queries []*models.CeramicQuery) (map[string]*models.StreamState, error) {
	type streamQuery struct {
		StreamId string `json:"streamId"`
	}
	type multiquery struct {
		Queries []*streamQuery `json:"queries"`
	}
	mq := multiquery{make([]*streamQuery, len(queries))}
	for idx, query := range queries {
		mq.Queries[idx] = &streamQuery{c.MultiqueryId(query)}
	}
	mqBody, err := json.Marshal(mq)
	if err != nil {
		log.Printf("multiquery: error creating request json: %v", err)
		return nil, err
	}
	log.Printf("multiquery: %s", mqBody)

	mqCtx, cancel := context.WithTimeout(context.Background(), models.CeramicMultiqueryTimeout)
	defer cancel()

	req, err := http.NewRequestWithContext(mqCtx, "POST", c.url+"/api/v0/multiqueries", bytes.NewBuffer(mqBody))
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

func (c Client) MultiqueryId(query *models.CeramicQuery) string {
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
