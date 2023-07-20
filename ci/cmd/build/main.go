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

	"github.com/ceramicnetwork/go-cas"
	"github.com/ceramicnetwork/go-cas/common/aws/config"
)

const ecrUserName = "AWS"

func main() {
	ctx := context.Background()

	client, err := dagger.Connect(ctx, dagger.WithLogOutput(os.Stdout))
	if err != nil {
		panic(err)
	}
	defer client.Close()

	contextDir := client.Host().Directory(".")
	registry := os.Getenv(cas.Env_AwsAccountId) + ".dkr.ecr." + os.Getenv(cas.Env_AwsRegion) + ".amazonaws.com"
	envTag := os.Getenv(cas.Env_EnvTag)
	container := contextDir.DockerBuild(dagger.DirectoryDockerBuildOpts{Platform: "linux/amd64"})
	tags := []string{
		envTag,
		os.Getenv(cas.Env_Branch),
		os.Getenv(cas.Env_Sha),
		os.Getenv(cas.Env_ShaTag),
	}
	// Only production images get the "latest" tag
	if envTag == cas.EnvTag_Prod {
		tags = append(tags, "latest")
	} else if envTag == cas.EnvTag_Dev {
		tags = append(tags, cas.EnvTag_Qa) // additionally tag with "qa" for images built from the "develop" branch
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
	awsCfg, err := config.AwsConfig(ctx)
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
		return strings.TrimPrefix(string(authToken), ecrUserName+":")
	}
}
