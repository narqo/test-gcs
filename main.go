package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"net/http"
	"os"
)

func main() {
	credsFile := flag.String("google_creds", "gcloud/application_default_credentials.json", "Path to Google Application Credentials json")

	flag.Parse()

	if err := run(*credsFile, flag.Args()); err != nil {
		fmt.Fprintf(os.Stderr, "%v\n", err)
		os.Exit(1)
	}
}

func run(credsFile string, files []string) error {
	if len(files) == 0 {
		return errors.New("no file to run")
	}

	ctx := context.Background()

	settingsRepo, err := NewSettingsRepo(credsFile)
	if err != nil {
		return err
	}

	httpClient := &http.Client{
		// TODO: http client settings
	}

	uploader := Uploader{
		httpClient: httpClient,
		settings:   settingsRepo,
	}

	for _, file := range files {
		fmt.Fprintf(os.Stdout, "uploading file %q...\n", file)
		if err := uploader.Upload(ctx, file); err != nil {
			return fmt.Errorf("failed uploading file %q: %v", file, err)
		}
	}
	return nil
}
