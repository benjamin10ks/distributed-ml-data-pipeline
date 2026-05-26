package file

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

type S3Adapter struct {
	client        *s3.Client
	landingBucket string
	events        chan RawEvent
	logger        *slog.Logger
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

func NewS3Adapter(landingBucket, endpoint, accessKey, secrectKey string, logger *slog.Logger) (*S3Adapter, error) {
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
		client:        client,
		landingBucket: landingBucket,
		events:        make(chan RawEvent, 256),
		logger:        logger,
	}, nil
}

func (s *S3Adapter) Register(mux *http.ServeMux) {
	mux.HandleFunc("POST /minio/events", s.handleNotificaion)
}

func (s *S3Adapter) Events() <-chan RawEvent { return s.events }

func (s *S3Adapter) handleNotificaion(w http.ResponseWriter, r *http.Request) {
	var notification minioNotification
	if err := json.NewDecoder(r.Body).Decode(&notification); err != nil {
		s.logger.Error("failed to decode notification", "error", err)
		http.Error(w, "invalid notification format", http.StatusBadRequest)
		return
	}

	for _, rec := range notification.Records {
		key := rec.S3.Object.Key
		bucket := rec.S3.Bucket.Name

		s.logger.Info("received notification", "bucket", bucket, "key", key)

		event, err := s.downloadObject(r.Context(), bucket, key)
		if err != nil {
			s.logger.Error("failed to download object", "bucket", bucket, "key", key, "error", err)
			continue
		}

		select {
		case s.events <- event:
		default:
			s.logger.Warn("events channel is full, dropping event", "bucket", bucket, "key", key)
		}
	}
	w.WriteHeader(http.StatusOK)
}

// Temporary will swap out later with temp files and chuncking for large files
func (s *S3Adapter) downloadObject(ctx context.Context, bucket, key string) (RawEvent, error) {
	resp, err := s.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return RawEvent{}, fmt.Errorf("failed to get object from S3: %w", err)
	}
	defer resp.Body.Close()

	h := sha256.New()
	body, err := io.ReadAll(io.TeeReader(resp.Body, h))
	if err != nil {
		return RawEvent{}, fmt.Errorf("failed to read object body: %w", err)
	}

	return RawEvent{
		Source:      "s3",
		Payload:     body,
		Format:      detectFormat(key, body),
		Path:        fmt.Sprintf("s3://%s/%s", bucket, key),
		ContentHash: hex.EncodeToString(h.Sum(nil)),
		Size:        int64(len(body)),
		ReceivedAt:  time.Now(),
		Metadata: map[string]string{
			"bucket": bucket,
			"key":    key,
		},
	}, nil
}

// sniffs for file type -- parquet, csv, ndjson. based on file extension or content
func detectFormat(key string, body []byte) string {
	if len(body) >= 4 && string(body[:4]) == "PAR1" {
		return "parquet"
	}

	if len(body) >= 2 && body[0] == 0x1f && body[1] == 0x8b {
		return "gzip"
	}

	// Fallback to file extension
	for i := len(key) - 1; i >= 0; i-- {
		if key[i] == '.' {
			ext := key[i+1:]
			switch ext {
			case "csv":
				return "csv"
			case "json", "ndjson", "jsonl":
				return "ndjson"
			case "parquet":
				return "parquet"
			}
			break
		}
	}

	return "unknown"
}
