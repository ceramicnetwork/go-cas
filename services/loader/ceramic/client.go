package ceramic

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io/ioutil"
	"log"
	"net/http"
	"os"
)

type CeramicClient struct {
	url string
}

func NewCeramicClient() *CeramicClient {
	return &CeramicClient{os.Getenv("CERAMIC_URL")}
}

func (c CeramicClient) query(ctx context.Context, streamId string) (*StreamState, error) {
	log.Printf("query: %s", streamId)

	qCtx, qCancel := context.WithTimeout(ctx, CeramicServerTimeout)
	defer qCancel()

	req, err := http.NewRequestWithContext(qCtx, "GET", c.url+"/api/v0/streams/"+streamId+"?sync=1", nil)
	//req, err := http.NewRequestWithContext(qCtx, "GET", c.url+"/api/v0/streams/"+streamId, nil)
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
	stream := Stream{}
	if err = json.Unmarshal(respBody, &stream); err != nil {
		log.Printf("query: error unmarshaling response: %v", err)
		return nil, err
	}
	stream.State.Id = streamId
	log.Printf("query: success: %+v", stream)
	return &stream.State, nil
}

func (c CeramicClient) multiquery(ctx context.Context, queries []*CidQuery) (map[string]*StreamState, error) {
	log.Printf("multiquery: %+v", queries)

	type streamQuery struct {
		StreamId string `json:"streamId"`
	}
	type multiquery struct {
		Queries []*streamQuery `json:"queries"`
	}
	mq := multiquery{make([]*streamQuery, len(queries))}
	for idx, query := range queries {
		mq.Queries[idx] = &streamQuery{query.mqId()}
	}
	mqBody, err := json.Marshal(mq)
	if err != nil {
		log.Printf("error creating multiquery json: %v", err)
		return nil, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), CeramicServerTimeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, "POST", c.url+"/api/v0/multiqueries?sync=1", bytes.NewBuffer(mqBody))
	if err != nil {
		log.Printf("error creating multiquery request: %v", err)
		return nil, err
	}
	req.Header.Add("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		log.Printf("error submitting multiquery: %v", err)
		return nil, err
	}
	defer resp.Body.Close()
	respBody, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		log.Printf("error reading multiquery response: %v", err)
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		log.Printf("error in multiquery: %v", resp.StatusCode)
		return nil, errors.New("multiquery: error in response")
	}
	mqResp := make(map[string]*StreamState)
	if err = json.Unmarshal(respBody, &mqResp); err != nil {
		log.Printf("error unmarshaling multiquery response: %v", err)
		return nil, err
	}
	log.Printf("mq response: streams=%d, resp=%+v", len(mqResp), mqResp)
	return mqResp, nil
}
