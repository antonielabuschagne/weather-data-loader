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

func NewWeatherAPIClient(apiKey string, baseUrl string, log *zapray.Logger) (c WeatherAPIClient, err error) {
	url, err := url.Parse(baseUrl)
	if err != nil {
		return
	}
	c.Log = log
	c.URL = url
	c.APIKey = apiKey
	c.Client = &http.Client{
		Timeout: 20 * time.Second,
	}
	return
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

func (c *WeatherAPIClient) GetWeatherForLatLong(ctx context.Context, lon, lat string) (result WeatherAPIResponse, err error) {
	reqUrl := c.buildUrl(map[string]string{"lon": lon, "lat": lat})

	c.Log.Info("sending weatherapi request")
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, reqUrl.String(), nil)
	if err != nil {
		return
	}
	res, err := c.Client.Do(req)
	if err != nil {
		return
	}
	defer res.Body.Close()
	if statusOK := res.StatusCode >= 200 && res.StatusCode < 300; !statusOK {
		body, _ := io.ReadAll(res.Body)
		err = fmt.Errorf("api failed to respond with a 2xx status code, got: %d. body: %s", res.StatusCode, string(body))
		return
	}
	err = json.NewDecoder(res.Body).Decode(&result)
	return
}
