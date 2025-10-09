package processors

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"errors"
	"io"
	"strings"

	"github.com/antonielabuschagne/data-loader/weatherapi"
	"github.com/aws/aws-lambda-go/events"
	"github.com/joerdav/zapray"
	"go.uber.org/zap"
)

type EventProcessor interface {
	Process(ctx context.Context, e events.S3Event) (processed []string, err error)
}

type DataFetcherFunc func(ctx context.Context, key string) (rc io.ReadCloser, err error)
type EventNotifierFunc func(ctx context.Context)
type MessageQueueFunc func(ctx context.Context, message string) (messageId string, err error)

type S3EventProcessor struct {
	DataFetcher  DataFetcherFunc
	Log          *zapray.Logger
	MessageQueue MessageQueueFunc
}

func NewS3EventProcessor(df DataFetcherFunc, mq MessageQueueFunc, log *zapray.Logger) S3EventProcessor {
	return S3EventProcessor{
		DataFetcher:  df,
		MessageQueue: mq,
		Log:          log,
	}
}

func (ep S3EventProcessor) Process(ctx context.Context, e events.S3Event) ([]string, error) {
	log := ep.Log
	processed := make([]string, 0, len(e.Records))
	for _, r := range e.Records {
		key := r.S3.Object.Key
		log.Info("processing s3 event", zap.String("key", key))
		if !strings.HasSuffix(key, ".csv") {
			log.Warn("skipping file extension")
			continue
		}
		messages, err := ep.processFile(ctx, key)
		if err != nil {
			log.Error("unable to process file", zap.String("error", err.Error()))
			continue
		}
		processed = append(processed, messages...)
		log.Info("messages processed", zap.Any("messages", messages))
	}
	return processed, nil
}

func (ep S3EventProcessor) processFile(ctx context.Context, key string) ([]string, error) {
	log := ep.Log
	lines, err := ep.getS3FileContent(ctx, key)
	if err != nil {
		log.Error("unable to get file content", zap.String("error", err.Error()))
		return nil, err
	}
	if len(lines) <= 1 {
		log.Info("file content empty (first row reserved for column heading)")
		return nil, nil
	}

	processed := make([]string, 0, len(lines)-1) // minus header row
	log.Info("processing CSV file", zap.Int("rows", len(lines)))
	for _, line := range lines[1:] {
		messageId, err := ep.addToMessageQueue(ctx, line)
		if err != nil {
			log.Error("unable to add message to queue", zap.String("error", err.Error()))
			continue
		}
		processed = append(processed, messageId)
	}
	return processed, nil
}

func (ep S3EventProcessor) addToMessageQueue(ctx context.Context, line []string) (string, error) {
	log := ep.Log
	message, err := convertRowToMessage(line)
	if err != nil {
		log.Error("unable to convert row into message", zap.String("error", err.Error()))
		return "", err
	}
	log.Info("message details", zap.String("body", message))
	messageId, err := ep.MessageQueue(ctx, message)
	if err != nil {
		log.Error("unable to send message", zap.String("error", err.Error()))
		return "", err
	}
	log.Info("message queued", zap.String("messageId", messageId))
	return messageId, nil
}

func (ep S3EventProcessor) getS3FileContent(ctx context.Context, key string) (lines [][]string, err error) {
	log := ep.Log
	log.Info("processing entry", zap.String("key", key))
	r, err := ep.DataFetcher(ctx, key)
	if err != nil {
		log.Error("unable to fetch data for key", zap.String("key", key))
		return
	}
	defer r.Close()

	lines, err = csv.NewReader(r).ReadAll()
	return
}

func convertRowToMessage(row []string) (message string, err error) {
	if len(row) < 2 {
		err = errors.New("invalid row, lon/lat required")
		return
	}
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
