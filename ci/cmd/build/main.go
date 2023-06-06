package main

import (
	"context"
	"encoding/base64"
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/aws/aws-sdk-go-v2/service/ecr"

	"dagger.io/dagger"

	"github.com/ceramicnetwork/go-cas/common/aws"
)

const EcrUserName = "AWS"

const (
	Env_EnvTag       = "ENV_TAG"
	Env_AwsAccountId = "AWS_ACCOUNT_ID"
	Env_AWS_REGION   = "AWS_REGION"
)

func main() {
	ctx := context.Background()

	client, err := dagger.Connect(ctx, dagger.WithLogOutput(os.Stdout))
	if err != nil {
		panic(err)
	}
	defer client.Close()

	contextDir := client.Host().Directory(".")
	ecrUri := os.Getenv(Env_AwsAccountId) + ".dkr.ecr." + os.Getenv(Env_AWS_REGION) + ".amazonaws.com"
	envTag := os.Getenv(Env_EnvTag)
	ref, err := contextDir.
		DockerBuild(dagger.DirectoryDockerBuildOpts{
			BuildArgs: []dagger.BuildArg{{Name: Env_EnvTag, Value: envTag}},
		}).
		WithRegistryAuth(
			ecrUri,
			"AWS",
			client.SetSecret("EcrAuthToken", getEcrToken(ctx)),
		).
		Publish(ctx, fmt.Sprintf(fmt.Sprintf("%s/app-cas-scheduler:latest", ecrUri)))
	if err != nil {
		panic(err)
	}

	fmt.Printf("build: published image to: %s", ref)
}

func getEcrToken(ctx context.Context) string {
	awsCfg, err := aws.AwsConfig()
	if err != nil {
		log.Fatalf("build: error creating aws cfg: %v", err)
	}
	ecrClient := ecr.NewFromConfig(awsCfg)
	if ecrTokenOut, err := ecrClient.GetAuthorizationToken(ctx, &ecr.GetAuthorizationTokenInput{}); err != nil {
		log.Fatalf("build: error retrieving ecr auth token: %v", err)
		return ""
	} else if authToken, err := base64.StdEncoding.DecodeString(*ecrTokenOut.AuthorizationData[0].AuthorizationToken); err != nil {
		log.Fatalf("build: error decoding ecr auth token: %v", err)
		return ""
	} else {
		return strings.TrimPrefix(string(authToken), EcrUserName+":")
	}
}
