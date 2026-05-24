package file

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

type S3Adapter struct {
	client     *s3.Client
	bucket     string
	listenAddr string
	events     chan RawEvent
	logger     *slog.Logger
}

type minioNotification struct {
	Records []struct {
		S3 struct {
			Bucket struct {
				Name string `json:"name"`
			} `json:"bucket"`
			Object struct {
				Key  string `json:"key"`
				Size int64  `json:"size"`
				ETag string `json:"eTag"`
			} `json:"object"`
		} `json:"s3"`
	} `json:"Records"`
}

func NewS3Adapter(listenAddr, bucket, endpoint, accessKey, secrectKey string, logger *slog.Logger) (*S3Adapter, error) {
	cfg, err := config.LoadDefaultConfig(context.Background(),
		config.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(accessKey, secrectKey, ""),
		),
		config.WithRegion("us-east-1"))
	if err != nil {
		return nil, fmt.Errorf("failed to load AWS config: %w", err)
	}

	client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(endpoint)
		o.UsePathStyle = true
	})

	return &S3Adapter{
		client:     client,
		bucket:     bucket,
		listenAddr: listenAddr,
		events:     make(chan RawEvent, 256),
		logger:     logger,
	}, nil
}

func (s *S3Adapter) Start(ctx context.Context) error {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /minio/events", s.handleNotificaion)

	srv := &http.Server{
		Addr:    s.listenAddr,
		Handler: mux,
	}

	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdownCtx)
	}()

	s.logger.Info("starting S3 adapter", "listenAddr", s.listenAddr)
	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		return fmt.Errorf("failed to start HTTP server: %w", err)
	}
	return nil
}

func (s *S3Adapter) Events() <-chan RawEvent { return s.events }

func (s *S3Adapter) handleNotificaion(w http.ResponseWriter, r *http.Request) {
	var notification minioNotification
	if err := json.NewDecoder(r.Body).Decode(&notification); err != nil {
		s.logger.Error("failed to decode notification", "error", err)
		http.Error(w, "invalid notification format", http.StatusBadRequest)
		return
	}
	//
}

// Temporary will swap out later with temp files and chuncking for large files
func (s *S3Adapter) downloadObject(ctx context.Context, butcket, key string) (RawEvent, error) {
	return RawEvent{}, nil
}

// sniffs for file type -- parquet, csv, ndjson. based on file extension or content
func detectFormat(key string, body []byte) string {
	return "unknown"
}
