package main

import (
	"context"
	"log"
	"os"

	"dagger.io/dagger"
)

func main() {
	ctx := context.Background()

	// Initialize the Dagger client
	client, err := dagger.Connect(ctx, dagger.WithLogOutput(os.Stdout))
	if err != nil {
		panic(err)
	}
	defer client.Close()

	// Use a golang:1.19 container, and mount the source code directory from the host at /src in the container.
	source := client.Container().
		From("golang:1.19").
		WithDirectory(
			"/src",
			client.Host().Directory("../../../"), dagger.ContainerWithDirectoryOpts{
				Exclude: []string{"ci/"},
			},
		)

	// Set the working directory in the container
	runner := source.WithWorkdir("/src/services")

	// Run application tests
	out, err := runner.WithExec([]string{"go", "test"}).Stderr(ctx)
	if err != nil {
		log.Fatalf("test: error running tests [%v]", err)
	}
	log.Printf("test: finished running tests [%s]", out)
}
