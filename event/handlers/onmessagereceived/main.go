package main

import (
	"context"
	"os"

	"github.com/antonielabuschagne/data-loader/event/processors"
	"github.com/antonielabuschagne/data-loader/weatherapi"
	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/joerdav/zapray"
	"go.uber.org/zap"
)

func main() {
	log, err := zapray.NewProduction()
	if err != nil {
		panic("unable to build logger")
	}
	weatherApiEndpoint := os.Getenv("WEATHER_API_ENDPOINT")
	if weatherApiEndpoint == "" {
		panic("WEATHER_API_ENDPOINT not configured")
	}
	weatherApiKey := os.Getenv("WEATHER_API_KEY")
	if weatherApiKey == "" {
		panic("WEATHER_API_KEY not configured")
	}
	wc, err := weatherapi.NewWeatherAPIClient(weatherApiKey, weatherApiEndpoint, log)
	if err != nil {
		panic("unable to build weather API client")
	}
	mp := processors.NewMessageProcessor(log, wc.GetWeatherForLatLong)
	h := NewHandler(log, mp)
	lambda.Start(h.handler)
}

type Handler struct {
	Log              *zapray.Logger
	MessageProcessor processors.MessageProcessor
}

func NewHandler(log *zapray.Logger, mp processors.MessageProcessor) Handler {
	return Handler{
		Log:              log,
		MessageProcessor: mp,
	}
}

func (h *Handler) handler(ctx context.Context, e events.SQSEvent) (err error) {
	log := h.Log
	mp := h.MessageProcessor
	log.Info("starting handler", zap.Int("records", len(e.Records)))
	processed := make([]string, len(e.Records))
	for _, r := range e.Records {
		err = mp.Process(ctx, r.Body)
		if err != nil {
			log.Error("unable to process message", zap.String("error", err.Error()))
			return
		}
		processed = append(processed, r.MessageId)
	}
	log.Info("weather requests processed", zap.Int("count", len(processed)))
	return
}
