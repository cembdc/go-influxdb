package main

import "time"

type ThermostatSetting struct {
	user string
	max  float64 //temperature
	avg  float64 //temperature
}

// Time: 2023-03-23 16:12:00, Symbol: BTCUSDT, Open: 27469.090000, Close: 27479.900000, High: 27483.920000, Low: 27469.080000
type BinanceAsset struct {
	Time   time.Time
	Symbol string
	Open   float64
	Close  float64
	High   float64
	Low    float64
}
