package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"

	"github.com/alexflint/go-arg"
)

func main() {
	var args struct {
		Status      string `arg:"-s,--status" help:"commit status"`
		RunUrl      string `arg:"env:RUN_URL" help:"GitHub workflow run URL"`
		StatusUrl   string `arg:"env:STATUS_URL" help:"GitHub commit status URL"`
		GitHubToken string `arg:"env:GH_TOKEN" help:"GitHub auth token"`
	}
	arg.MustParse(&args)
	if err := updateCommitStatus(args.Status, args.StatusUrl, args.RunUrl); err != nil {
		log.Fatalf("status: error publishing status [%v]", err)
	}
}

func updateCommitStatus(status, statusUrl, targetUrl string) error {
	reqBody, _ := json.Marshal(map[string]string{
		"state":      status,
		"target_url": targetUrl,
	})
	resp, err := http.Post(statusUrl, "application/vnd.github.v3+json", bytes.NewBuffer(reqBody))
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	respBody := string(body)
	if !strings.Contains(respBody, status) {
		return fmt.Errorf("expected status %s missing in %s", status, respBody)
	}
	return nil
}
