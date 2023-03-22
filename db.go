package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"time"

	influxdb2 "github.com/influxdata/influxdb-client-go/v2"
)

var org = "local"
var bucket = "test"

// Connect to an Influx Database reading the credentials from
// environment variables INFLUXDB_TOKEN, INFLUXDB_URL
// return influxdb Client or errors
func ConnectToInfluxDB() (influxdb2.Client, error) {

	dbToken := os.Getenv("INFLUXDB_TOKEN")
	if dbToken == "" {
		return nil, errors.New("INFLUXDB_TOKEN must be set")
	}

	dbURL := os.Getenv("INFLUXDB_URL")
	if dbURL == "" {
		return nil, errors.New("INFLUXDB_URL must be set")
	}

	client := influxdb2.NewClient(dbURL, dbToken)

	// validate client connection health
	_, err := client.Health(context.Background())

	return client, err
}

func write_event_with_line_protocol(client influxdb2.Client, t ThermostatSetting) {
	// get non-blocking write client
	writeAPI := client.WriteAPI(org, bucket)
	// write line protocol
	writeAPI.WriteRecord(fmt.Sprintf("thermostat,unit=temperature,user=%s avg=%f,max=%f", t.user, t.avg, t.max))
	// Flush writes
	writeAPI.Flush()
}

func write_event_with_params_constror(client influxdb2.Client, t ThermostatSetting) {
	// Use blocking write client for writes to desired bucket
	writeAPI := client.WriteAPI(org, bucket)
	// Create point using full params constructor
	p := influxdb2.NewPoint("thermostat",
		map[string]string{"unit": "temperature", "user": t.user},
		map[string]interface{}{"avg": t.avg, "max": t.max},
		time.Now())
	writeAPI.WritePoint(p)
	// Flush writes
	writeAPI.Flush()
}

func write_event_with_fluent_Style(client influxdb2.Client, t ThermostatSetting) {
	// Use blocking write client for writes to desired bucket
	writeAPI := client.WriteAPI(org, bucket)
	// create point using fluent style
	p := influxdb2.NewPointWithMeasurement("thermostat").
		AddTag("unit", "temperature").
		AddTag("user", t.user).
		AddField("avg", t.avg).
		AddField("max", t.max).
		SetTime(time.Now())
	writeAPI.WritePoint(p)
	// Flush writes
	writeAPI.Flush()
}