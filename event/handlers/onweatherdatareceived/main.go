package main

import (
	"context"
	"os"

	"github.com/antonielabuschagne/data-loader/event/processors"
	"github.com/antonielabuschagne/data-loader/messagequeue"
	"github.com/antonielabuschagne/data-loader/s3client"
	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/joerdav/zapray"
	"go.uber.org/zap"
)

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
	cfg, err := config.LoadDefaultConfig(context.Background())
	if err != nil {
		log.Fatal("error loading config")
	}
	// our handler needs an EventProcessor that will do something with the event data and return a message
	// id as receipt of delivery. What our S3EventProcessor needs to do that, is a fetcher for fetching the
	// data and a message queue for delivering the data somewhere. That's the extend to what it cares about.
	messageQueue := messagequeue.NewMessageQueue(cfg, queueUrl)
	fetcher := s3client.NewS3DataFetcher(cfg, bucket)
	processor := processors.NewS3EventProcessor(fetcher, messageQueue.SendMessage, log)

	h := NewHandler(log, processor)
	lambda.Start(h.handler)
}

type Handler struct {
	Log            *zapray.Logger
	EventProcessor processors.EventProcessor
}

func NewHandler(log *zapray.Logger, ep processors.EventProcessor) Handler {
	return Handler{
		Log:            log,
		EventProcessor: ep,
	}
}

func (h *Handler) handler(ctx context.Context, e events.S3Event) (err error) {
	h.Log.Info("processing weather data", zap.Int("records", len(e.Records)))
	messages, err := h.EventProcessor.Process(ctx, e)
	if err != nil {
		h.Log.Error("unable to process file", zap.String("error", err.Error()))
	}
	h.Log.Info("event processed completed", zap.Any("messages", messages))
	return
}
