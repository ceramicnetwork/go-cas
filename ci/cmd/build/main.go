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

	"github.com/ceramicnetwork/go-cas/common/aws/config"
)

const EcrUserName = "AWS"

const (
	Env_EnvTag       = "ENV_TAG"
	Env_AwsAccountId = "AWS_ACCOUNT_ID"
	Env_AwsRegion    = "AWS_REGION"
)

const (
	EnvTag_Dev  = "dev"
	EnvTag_Qa   = "qa"
	EnvTag_Tnet = "tnet"
	EnvTag_Prod = "prod"
)

func main() {
	ctx := context.Background()

	client, err := dagger.Connect(ctx, dagger.WithLogOutput(os.Stdout))
	if err != nil {
		panic(err)
	}
	defer client.Close()

	contextDir := client.Host().Directory(".")
	registry := os.Getenv(Env_AwsAccountId) + ".dkr.ecr." + os.Getenv(Env_AwsRegion) + ".amazonaws.com"
	envTag := os.Getenv(Env_EnvTag)
	container := contextDir.
		DockerBuild(dagger.DirectoryDockerBuildOpts{
			Platform:  "linux/amd64",
			BuildArgs: []dagger.BuildArg{{Name: Env_EnvTag, Value: envTag}},
		})
	tags := []string{
		envTag,
		os.Getenv("BRANCH"),
		os.Getenv("SHA"),
		os.Getenv("SHA_TAG"),
	}
	// Only production images get the "latest" tag
	if envTag == EnvTag_Prod {
		tags = append(tags, "latest")
	}
	if err = pushImage(ctx, client, container, registry, tags); err != nil {
		log.Fatalf("build: failed to push image: %v", err)
	}
}

func pushImage(ctx context.Context, client *dagger.Client, container *dagger.Container, registry string, tags []string) error {
	// Set up registry authentication
	ecrToken := client.SetSecret("EcrAuthToken", getEcrToken(ctx))
	container = container.WithRegistryAuth(registry, "AWS", ecrToken)
	for _, tag := range tags {
		if _, err := container.Publish(ctx, fmt.Sprintf(fmt.Sprintf("%s/app-cas-scheduler:%s", registry, tag))); err != nil {
			return err
		}
	}
	return nil
}

func getEcrToken(ctx context.Context) string {
	awsCfg, err := config.AwsConfig()
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
