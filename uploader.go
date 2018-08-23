package main

import (
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"path/filepath"
	"regexp"

	"cloud.google.com/go/storage"
	"golang.org/x/oauth2/google"
	"google.golang.org/api/option"
)

var fileNameRegex = regexp.MustCompile(`^(\w{12})_([0-9T:-]+)_(\w{32}).csv(.gz)?$`)

const (
	credsGCS = "googlestorage"

	testBucket = "backend_test"
)

type Credentials struct {
	Type   string
	Bucket string
	JSON   []byte
}

type Settings struct {
	Credentials Credentials
}

type SettingsRepo struct {
	jsonData []byte
}

func NewSettingsRepo(credsFile string) (*SettingsRepo, error) {
	b, err := ioutil.ReadFile(credsFile)
	if err != nil {
		return nil, fmt.Errorf("failed to read google credentials file %q: %v", credsFile, err)
	}
	repo := &SettingsRepo{
		jsonData: b,
	}
	return repo, nil
}

func (repo *SettingsRepo) GetSettings(token string) (Settings, error) {
	settings := Settings{
		Credentials: Credentials{
			Type:   credsGCS,
			Bucket: testBucket,
			JSON:   repo.jsonData,
		},
	}
	return settings, nil
}

type Uploader struct {
	httpClient *http.Client
	settings   *SettingsRepo
}

func (u *Uploader) Upload(ctx context.Context, filePath string) error {
	fileName := filepath.Base(filePath)
	matches := fileNameRegex.FindStringSubmatch(fileName)
	if len(matches) != 5 {
		return fmt.Errorf("file name %q does not match the pattern", fileName)
	}

	appToken := matches[1]

	settings, err := u.settings.GetSettings(appToken)
	if err != nil {
		return fmt.Errorf("could not get settings for %q: %v", appToken, err)
	}

	if matches[4] == "" {
		// fail for now
		return fmt.Errorf("skip non-tgz file %q", fileName)
	}

	if settings.Credentials.Type != credsGCS {
		return fmt.Errorf("unknown credentials type: %s", settings.Credentials.Type)
	}

	return u.upload(ctx, settings.Credentials, filePath, fileName)
}

func (u *Uploader) readCredentials(ctx context.Context, jsonData []byte) (*google.Credentials, error) {
	return google.CredentialsFromJSON(ctx, jsonData, storage.ScopeReadWrite)
}

func (u *Uploader) upload(ctx context.Context, creds Credentials, filePath, objName string) error {
	clientCreds, err := u.readCredentials(ctx, creds.JSON)
	if err != nil {
		return fmt.Errorf("could not read credentials: %v", err)
	}

	client, err := storage.NewClient(
		ctx,
		//option.WithHTTPClient(u.httpClient), // TODO: client overrides creds. Need to build creds aware client manually
		option.WithCredentials(clientCreds),
	)
	if err != nil {
		return fmt.Errorf("could not create storage client: %v", err)
	}
	defer client.Close()

	f, err := os.Open(filePath)
	if err != nil {
		return err
	}
	defer f.Close()

	ctx, cancel := context.WithCancel(ctx)

	w := client.Bucket(creds.Bucket).Object(objName).NewWriter(ctx)
	if _, err := io.Copy(w, f); err != nil {
		// cancel writer's context to abort the write operation
		cancel()
		return fmt.Errorf("could not upload file %q to bucket %q: %v", filePath, creds.Bucket, err)
	}
	return w.Close()
}
