package processors

import (
	"context"
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/aws/aws-lambda-go/events"
	"github.com/google/uuid"
	"github.com/joerdav/zapray"
)

func TestNewS3EventProcessor(t *testing.T) {
	logger, err := zapray.NewDevelopment()
	if err != nil {
		t.Fatal("unable to create logger")
	}

	tests := []struct {
		fetcher       DataFetcherFunc
		messageQueue  MessageQueueFunc
		s3event       events.S3Event
		description   string
		expectedCount int
	}{
		{
			description: "given single line of coordinates, returns messageId",
			fetcher: func(ctx context.Context, key string) (rc io.ReadCloser, err error) {
				sr := strings.NewReader("lon,lat\n1,2")
				rc = io.NopCloser(sr)
				return
			},
			messageQueue: func(ctx context.Context, message string) (messageId string, err error) {
				messageId = uuid.New().String()
				return
			},
			s3event: events.S3Event{
				Records: []events.S3EventRecord{
					{
						S3: events.S3Entity{
							Object: events.S3Object{
								Key: "data.csv",
							},
						},
					},
				},
			},
			expectedCount: 1,
		},
		{
			description: "given multiple lines of coordinates, returns messageId's",
			fetcher: func(ctx context.Context, key string) (rc io.ReadCloser, err error) {
				sr := strings.NewReader("lon,lat\n1,2\n2,4\n5,6")
				rc = io.NopCloser(sr)
				return
			},
			messageQueue: func(ctx context.Context, message string) (messageId string, err error) {
				messageId = uuid.New().String()
				return
			},
			s3event: events.S3Event{
				Records: []events.S3EventRecord{
					{
						S3: events.S3Entity{
							Object: events.S3Object{
								Key: "data.csv",
							},
						},
					},
				},
			},
			expectedCount: 3,
		},
		{
			description: "given multiple files, returns messageId's for each of the lines",
			fetcher: func(ctx context.Context, key string) (rc io.ReadCloser, err error) {
				sr := strings.NewReader("lon,lat\n1,2\n2,4\n5,6")
				rc = io.NopCloser(sr)
				return
			},
			messageQueue: func(ctx context.Context, message string) (messageId string, err error) {
				messageId = uuid.New().String()
				return
			},
			s3event: events.S3Event{
				Records: []events.S3EventRecord{
					{
						S3: events.S3Entity{
							Object: events.S3Object{
								Key: "data.csv",
							},
						},
					},
					{
						S3: events.S3Entity{
							Object: events.S3Object{
								Key: "data.csv",
							},
						},
					},
				},
			},
			expectedCount: 6,
		},
		{
			description: "given just a csv heading row, no messages delivered",
			fetcher: func(ctx context.Context, key string) (rc io.ReadCloser, err error) {
				sr := strings.NewReader("lon,lat")
				rc = io.NopCloser(sr)
				return
			},
			messageQueue: func(ctx context.Context, message string) (messageId string, err error) {
				messageId = uuid.New().String()
				return
			},
			s3event: events.S3Event{
				Records: []events.S3EventRecord{
					{
						S3: events.S3Entity{
							Object: events.S3Object{
								Key: "data.csv",
							},
						},
					},
				},
			},
			expectedCount: 0,
		},
		{
			description: "given wrong key suffix, no messages delivered",
			fetcher: func(ctx context.Context, key string) (rc io.ReadCloser, err error) {
				sr := strings.NewReader("lon,lat\n1,2\n3,4")
				rc = io.NopCloser(sr)
				return
			},
			messageQueue: func(ctx context.Context, message string) (messageId string, err error) {
				messageId = uuid.New().String()
				return
			},
			s3event: events.S3Event{
				Records: []events.S3EventRecord{
					{
						S3: events.S3Entity{
							Object: events.S3Object{
								Key: "data.txt",
							},
						},
					},
				},
			},
			expectedCount: 0,
		},
		{
			description: "given fetcher returns an error, no messages delivered",
			fetcher: func(ctx context.Context, key string) (rc io.ReadCloser, err error) {
				err = errors.New("unable to fetch data")
				return
			},
			messageQueue: func(ctx context.Context, message string) (messageId string, err error) {
				messageId = uuid.New().String()
				return
			},
			s3event: events.S3Event{
				Records: []events.S3EventRecord{
					{
						S3: events.S3Entity{
							Object: events.S3Object{
								Key: "data.txt",
							},
						},
					},
				},
			},
			expectedCount: 0,
		},
		{
			description: "given message queue is unavailable, no messages delivered",
			fetcher: func(ctx context.Context, key string) (rc io.ReadCloser, err error) {
				sr := strings.NewReader("lon,lat\n1,2\n3,4")
				rc = io.NopCloser(sr)
				return
			},
			messageQueue: func(ctx context.Context, message string) (messageId string, err error) {
				err = errors.New("message queue unavailable")
				return
			},
			s3event: events.S3Event{
				Records: []events.S3EventRecord{
					{
						S3: events.S3Entity{
							Object: events.S3Object{
								Key: "data.txt",
							},
						},
					},
				},
			},
			expectedCount: 0,
		},
	}

	for _, tt := range tests {
		ep := NewS3EventProcessor(tt.fetcher, tt.messageQueue, logger)
		messages, err := ep.Process(context.Background(), tt.s3event)
		if err != nil {
			t.Errorf("unable to process: %s", err.Error())
		}
		if len(messages) != tt.expectedCount {
			t.Errorf("expected %d messageId's, got %d", tt.expectedCount, len(messages))
		}
	}
}
