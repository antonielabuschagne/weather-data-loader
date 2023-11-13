package processors

import (
	"context"
	"encoding/json"
	"errors"

	weather "github.com/antonielabuschagne/data-loader/weatherapi"
	"github.com/joerdav/zapray"
	"go.uber.org/zap"
)

type WeatherFetcherFunc func(ctx context.Context, lon, lat string) (result weather.WeatherAPIResponse, err error)

type MessageProcessor struct {
	Log           *zapray.Logger
	WeatherClient WeatherFetcherFunc
}

func NewMessageProcessor(log *zapray.Logger, wc WeatherFetcherFunc) (mp MessageProcessor) {
	mp.Log = log
	mp.WeatherClient = wc
	return
}

func (mp *MessageProcessor) Process(ctx context.Context, message string) (err error) {
	log := mp.Log
	var req weather.WeatherAPIRequest
	if err := json.Unmarshal([]byte(message), &req); err != nil {
		return err
	}
	if req.Lat == "" || req.Lon == "" {
		err = errors.New("invalid message, lon/lat required")
		return
	}
	res, err := mp.WeatherClient(ctx, req.Lon, req.Lat)
	if err != nil {
		log.Error("unable to query weather API", zap.String("error", err.Error()))
		return
	}
	log.Info("weather data retrieved", zap.String("description", res.WeatherResults[0].Description), zap.Float64("temp", res.Main.Temp))
	return
}
