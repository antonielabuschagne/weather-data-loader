package processors

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"errors"
	"io"

	"github.com/antonielabuschagne/data-loader/messagequeue"
	"github.com/antonielabuschagne/data-loader/weatherapi"
	"github.com/joerdav/zapray"
	"go.uber.org/zap"
)

type DataFetcher func(ctx context.Context, key string) (io.ReadCloser, error)
type EventNotifier func(ctx context.Context)

type S3EventProcessor struct {
	DataFetcher  DataFetcher
	Log          *zapray.Logger
	MessageQueue messagequeue.MessageQueue
}

func NewS3EventProcessor(df DataFetcher, mq messagequeue.MessageQueue, log *zapray.Logger) (p S3EventProcessor) {
	p.DataFetcher = df
	p.MessageQueue = mq
	p.Log = log
	return
}

func (ep *S3EventProcessor) Process(ctx context.Context, key string) (processed []string, err error) {
	log := ep.Log
	log.Info("processing incoming event data")
	r, err := ep.DataFetcher(ctx, key)
	if err != nil {
		log.Error("unable to fetch data")
		return
	}
	defer r.Close()

	lines, err := csv.NewReader(r).ReadAll()
	if len(lines) <= 1 {
		log.Info("file content empty (first row reserved for column heading)")
		return
	}
	log.Info("processing CSV file", zap.Int("rows", len(lines)))
	for _, row := range lines[1:] {
		message, err := convertRowToMessage(row)
		// TODO: should we process the next or halt all proceedings?
		if err != nil {
			log.Error("unable to convert row into message", zap.String("error", err.Error()))
			return nil, err
		}
		log.Info("message details", zap.String("body", message))
		// TODO: send in batch
		messageId, err := ep.MessageQueue.SendMessage(ctx, message)
		if err != nil {
			log.Error("unable to send message", zap.String("error", err.Error()))
			return nil, err
		}
		log.Info("message queued", zap.String("messageId", messageId))
		processed = append(processed, messageId)
	}
	return
}

func convertRowToMessage(row []string) (message string, err error) {
	lon := row[0]
	lat := row[1]
	if lon == "" || lat == "" {
		err = errors.New("bad data provided")
		return
	}
	wr := weatherapi.WeatherAPIRequest{
		Lon: lon,
		Lat: lat,
	}
	d, err := json.Marshal(wr)
	if err != nil {
		return
	}
	message = string(d)
	return
}
