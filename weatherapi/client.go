package weatherapi

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"

	"github.com/joerdav/zapray"
)

type WeatherAPIClient struct {
	Client *http.Client
	URL    *url.URL
	APIKey string
	Log    *zapray.Logger
}

func NewWeatherAPIClient(apiKey string, baseUrl string, log *zapray.Logger) (WeatherAPIClient, error) {
	url, err := url.Parse(baseUrl)
	if err != nil {
		return WeatherAPIClient{}, fmt.Errorf("failed to parse base url: %w", err)
	}
	return WeatherAPIClient{
		Log:    log,
		URL:    url,
		APIKey: apiKey,
		Client: &http.Client{
			Timeout: 20 * time.Second,
		},
	}, nil
}

func (c *WeatherAPIClient) buildUrl(params map[string]string) *url.URL {
	url := c.URL

	q := url.Query()
	q.Set("appid", c.APIKey)
	for k, v := range params {
		q.Set(k, v)
	}
	url.RawQuery = q.Encode()
	return url
}

func (c *WeatherAPIClient) GetWeatherForLatLong(ctx context.Context, lon, lat string) (WeatherAPIResponse, error) {
	reqUrl := c.buildUrl(map[string]string{"lon": lon, "lat": lat})

	c.Log.Info("sending weatherapi request")
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, reqUrl.String(), nil)
	if err != nil {
		return WeatherAPIResponse{}, fmt.Errorf("failed to create request: %w", err)
	}
	res, err := c.Client.Do(req)
	if err != nil {
		return WeatherAPIResponse{}, fmt.Errorf("failed to do request: %w", err)
	}
	defer res.Body.Close()
	if statusOK := res.StatusCode >= 200 && res.StatusCode < 300; !statusOK {
		body, _ := io.ReadAll(res.Body)
		err = fmt.Errorf("api failed to respond with a 2xx status code, got: %d. body: %s", res.StatusCode, string(body))
		return WeatherAPIResponse{}, err
	}
	var result WeatherAPIResponse
	err = json.NewDecoder(res.Body).Decode(&result)
	return result, err
}
