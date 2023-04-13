package processors

import (
	"context"
	"encoding/json"

	"github.com/antonielabuschagne/data-loader/weatherapi"
	"github.com/joerdav/zapray"
	"go.uber.org/zap"
)

type MessageProcessor struct {
	Log           *zapray.Logger
	WeatherClient weatherapi.WeatherAPIClient
}

func NewMessageProcessor(log *zapray.Logger, wc weatherapi.WeatherAPIClient) (mp MessageProcessor) {
	mp.Log = log
	mp.WeatherClient = wc
	return
}

func (mp *MessageProcessor) Process(ctx context.Context, message string) (err error) {
	log := mp.Log
	var req weatherapi.WeatherAPIRequest
	if err := json.Unmarshal([]byte(message), &req); err != nil {
		return err
	}
	res, err := mp.WeatherClient.GetWeatherForLatLong(ctx, req.Lon, req.Lat)
	if err != nil {
		log.Error("unable to query weather API", zap.String("error", err.Error()))
		return
	}
	log.Info("weather data retrieved", zap.String("description", res.WeatherResults[0].Description), zap.Float64("temp", res.Main.Temp))
	return
}
