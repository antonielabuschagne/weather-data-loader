package weatherapi

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/joerdav/zapray"
)

func TestGetWeatherForLatLong(t *testing.T) {
	tests := []struct {
		resBody       string
		resCode       int
		expected      WeatherAPIResponse
		expectedError string
	}{
		{
			resBody:  marshalResponse(t, buildGoodWeatherResponse()),
			resCode:  http.StatusOK,
			expected: buildGoodWeatherResponse(),
		}, {
			resBody:       "",
			resCode:       http.StatusUnauthorized,
			expectedError: "api failed to respond with a 2xx status code, got: 401. body: ",
		},
	}

	for _, tt := range tests {
		server := buildHttpTestServer(tt.resCode, tt.resBody)
		defer server.Close()
		client, err := buildWeatherApiClient(server.URL)
		if err != nil {
			t.Error(err)
		}

		res, err := client.GetWeatherForLatLong(context.Background(), "1", "2")
		if tt.expectedError != "" {
			if err.Error() != tt.expectedError {
				t.Errorf("got error %q, but expected error %q", err, tt.expectedError)
			}
			continue
		}
		if err != nil {
			t.Errorf("unable to get weather response: %v", err.Error())
		}
		if !cmp.Equal(res, tt.expected) {
			t.Errorf("got %v expected %v", res, tt.expected)
		}
	}
}

func buildWeatherApiClient(u string) (wa WeatherAPIClient, err error) {
	logger, err := zapray.NewDevelopment()
	if err != nil {
		return
	}
	wa, err = NewWeatherAPIClient("apikey", u, logger)
	return
}

func buildHttpTestServer(s int, b string) *httptest.Server {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(s)
		_, err := w.Write([]byte(b))
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
		}
		fmt.Printf("%+v", r.URL)
	}))
	return ts
}

func marshalResponse(t *testing.T, res WeatherAPIResponse) string {
	r, err := json.Marshal(res)
	if err != nil {
		t.Errorf("unable to marshall WeatherResponse: %v", err)
	}
	return string(r)
}

func buildGoodWeatherResponse() WeatherAPIResponse {
	return WeatherAPIResponse{
		Coorinates: Coorinates{
			Lon: 1,
			Lat: 2,
		},
		Main: Main{
			Temp:      20,
			TempMin:   10,
			TempMax:   30,
			FeelsLike: 25,
			Humidity:  90,
		},
		WeatherResults: []Weather{
			{
				ID:          1,
				Description: "warm",
				Main:        "main",
				Icon:        "icon",
			},
		},
	}
}
