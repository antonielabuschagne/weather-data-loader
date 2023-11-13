package processors

import (
	"context"
	"errors"
	"testing"

	weather "github.com/antonielabuschagne/data-loader/weatherapi"
	"github.com/joerdav/zapray"
)

func TestMessageProcessor(t *testing.T) {
	logger, err := zapray.NewDevelopment()
	if err != nil {
		t.Fatal("unable to create logger")
	}

	tests := []struct {
		weatherFetcher WeatherFetcherFunc
		description    string
		message        string
		expectedError  string
	}{
		{
			description: "given a valid message and weather API response, successful response returned",
			weatherFetcher: func(ctx context.Context, lon, lat string) (result weather.WeatherAPIResponse, err error) {
				result = buildGoodWeatherResponse()
				return
			},
			message: `{"lat": "123", "lon": "123"}`,
		},
		{
			description: "given a bad message data, failed response returned",
			weatherFetcher: func(ctx context.Context, lon, lat string) (result weather.WeatherAPIResponse, err error) {
				result = buildGoodWeatherResponse()
				return
			},
			message:       `{"lat": "123"}`,
			expectedError: "invalid message, lon/lat required",
		},
		{
			description: "given a bad message, failed response returned",
			weatherFetcher: func(ctx context.Context, lon, lat string) (result weather.WeatherAPIResponse, err error) {
				result = buildGoodWeatherResponse()
				return
			},
			message:       `{"lat": "123}`,
			expectedError: "unexpected end of JSON input",
		},
		{
			description: "given a good message and bad API response, failed response returned",
			weatherFetcher: func(ctx context.Context, lon, lat string) (result weather.WeatherAPIResponse, err error) {
				err = errors.New("rate limit exceeded")
				return
			},
			message:       `{"lat": "123", "lon": "123"}`,
			expectedError: "rate limit exceeded",
		},
	}

	for _, tt := range tests {
		mp := NewMessageProcessor(logger, tt.weatherFetcher)
		err := mp.Process(context.Background(), tt.message)

		if err != nil {
			if tt.expectedError == "" {
				t.Errorf("got error %s, but didn't expect error", err.Error())
			}
			if tt.expectedError != "" {
				if err.Error() != tt.expectedError {
					t.Errorf("got error %s, but expected error %s", err.Error(), tt.expectedError)
				}
			}
		}
	}
}

func buildGoodWeatherResponse() weather.WeatherAPIResponse {
	return weather.WeatherAPIResponse{
		Coorinates: weather.Coorinates{
			Lon: 1,
			Lat: 2,
		},
		Main: weather.Main{
			Temp:      20,
			TempMin:   10,
			TempMax:   30,
			FeelsLike: 25,
			Humidity:  90,
		},
		WeatherResults: []weather.Weather{
			{
				ID:          1,
				Description: "warm",
				Main:        "main",
				Icon:        "icon",
			},
		},
	}
}
