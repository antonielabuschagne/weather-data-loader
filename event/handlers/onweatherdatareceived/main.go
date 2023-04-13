package main

import (
	"context"
	"os"
	"strings"

	"github.com/antonielabuschagne/data-loader/event/processors"
	"github.com/antonielabuschagne/data-loader/messagequeue"
	"github.com/antonielabuschagne/data-loader/s3client"
	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/joerdav/zapray"
	"go.uber.org/zap"
)

type Handler struct {
	Log      *zapray.Logger
	Bucket   string
	QueueURL string
}

func NewHandler(log *zapray.Logger, bucket string, queueUrl string) Handler {
	return Handler{
		Log:      log,
		Bucket:   bucket,
		QueueURL: queueUrl,
	}
}

func main() {
	log, err := zapray.NewProduction()
	if err != nil {
		panic("unable to build logger")
	}
	queueUrl := os.Getenv("WEATHER_DATA_SQS_QUEUE_URL")
	if queueUrl == "" {
		log.Fatal("WEATHER_DATA_SQS_QUEUE_URL not defined")
	}
	bucket := os.Getenv("WEATHER_DATA_BUCKET_NAME")
	if bucket == "" {
		log.Fatal("WEATHER_DATA_BUCKET_NAME not defined")
	}

	h := NewHandler(log, bucket, queueUrl)
	lambda.Start(h.handler)
}

func (h *Handler) handler(ctx context.Context, e events.S3Event) (err error) {
	log := h.Log
	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		return
	}
	messageQueue := messagequeue.NewMessageQueue(cfg, h.QueueURL)
	fetcher := s3client.NewS3DataFetcher(cfg, h.Bucket)
	processor := processors.NewS3EventProcessor(fetcher, messageQueue, log)

	log.Info("processing weather data", zap.Int("records", len(e.Records)))
	for _, r := range e.Records {
		key := r.S3.Object.Key
		log.Info("processing s3 event", zap.String("key", key))
		if !strings.HasSuffix(key, ".csv") {
			log.Warn("not the correct file format to process")
			continue
		}
		messages, err := processor.Process(ctx, key)
		if err != nil {
			log.Error("unable to process file", zap.String("error", err.Error()))
		}
		log.Info("messages processed", zap.Any("messages", messages))
	}
	log.Info("event processed completed")
	return
}
