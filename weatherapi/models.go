package weatherapi

type WeatherAPIRequest struct {
	Lat string `json:"lat"`
	Lon string `json:"lon"`
}

type WeatherAPIResponse struct {
	Coorinates     Coorinates `json:"coord"`
	Main           Main       `json:"main"`
	WeatherResults []Weather  `json:"weather"`
}

type Coorinates struct {
	Lon float64 `json:"lon"`
	Lat float64 `json:"lat"`
}

type Weather struct {
	ID          int64  `json:"id"`
	Main        string `json:"main"`
	Description string `json:"description"`
	Icon        string `json:"icon"`
}

type Main struct {
	Temp      float64 `json:"temp"`
	TempMin   float64 `json:"temp_min"`
	TempMax   float64 `json:"temp_max"`
	FeelsLike float64 `json:"feels_like"`
	Humidity  int64   `json:"humidity"`
}
