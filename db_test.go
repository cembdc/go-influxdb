package main

import (
	"context"
	"reflect"
	"testing"
	"time"

	influxdb2 "github.com/influxdata/influxdb-client-go/v2"
	"github.com/influxdata/influxdb-client-go/v2/domain"
	"github.com/joho/godotenv"
)

// initialize the connection and clean the database before each test.
func init_testDB(t *testing.T) influxdb2.Client {
	t.Helper()                           // Tells `go test` that this is an helper
	godotenv.Load("./test_influxdb.env") //load environement variable
	client, err := ConnectToInfluxDB()   // create the client

	if err != nil {
		t.Errorf("impossible to connect to DB")
	}

	// Clean the database by deleting the bucket
	ctx := context.Background()
	bucketsAPI := client.BucketsAPI()
	dBucket, err := bucketsAPI.FindBucketByName(ctx, bucket)
	if err == nil {
		client.BucketsAPI().DeleteBucketWithID(context.Background(), *dBucket.Id)
	}

	// create new empty bucket
	dOrg, _ := client.OrganizationsAPI().FindOrganizationByName(ctx, org)
	_, err = client.BucketsAPI().CreateBucketWithNameWithID(ctx, *dOrg.Id, bucket)

	if err != nil {
		t.Errorf("impossible to new create bucket")
	}

	return client
}

func Test_connectToInfluxDB(t *testing.T) {

	//load environment variable from a file for test purposes
	godotenv.Load("./test_influxdb.env")

	tests := []struct {
		name    string
		wantErr bool
	}{
		{
			name:    "Successful connection to InfluxDB",
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ConnectToInfluxDB()
			if (err != nil) != tt.wantErr {
				t.Errorf("ConnectToInfluxDB() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			health, err := got.Health(context.Background())
			if (err != nil) && health.Status == domain.HealthCheckStatusPass {
				t.Errorf("connectToInfluxDB() error. database not healthy")
				return
			}
			got.Close()
		})
	}
}

func Test_write_event_with_line_protocol(t *testing.T) {
	tests := []struct {
		name  string
		f     func(influxdb2.Client, []ThermostatSetting)
		datas []ThermostatSetting
	}{
		{
			name: "Write new record with line protocol",
			// Your data Points
			datas: []ThermostatSetting{{user: "foo", avg: 35.5, max: 42}},
			f: func(c influxdb2.Client, datas []ThermostatSetting) {
				// Send all the data to the DB
				for _, data := range datas {
					write_event_with_line_protocol(c, data)
				}
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// helper to initialise and clean the database
			client := init_testDB(t)
			// call function under test
			tt.f(client, tt.datas)

			// test can be flicky if the query is done before that data is ready in the database
			time.Sleep(time.Millisecond * 1000)

			// Option one: QueryTableResult
			results := read_events_as_query_table_result(client)
			// convert results to array to compare with data
			resultsArr := []ThermostatSetting{}
			for _, v := range results {
				resultsArr = append(resultsArr, v)
			}

			if eq := reflect.DeepEqual(resultsArr, tt.datas); !eq {
				t.Errorf("want %v, got %v", tt.datas, resultsArr)
			}

			// Option two: query raw data

			// TODO add validation
			read_events_as_raw_string(client)

			client.Close()
		})
	}
}

func Test_write_coin_event_with_line_protocol(t *testing.T) {

	client := init_testDB(t)

	assets := []BinanceAsset{}
	assets = []BinanceAsset{
		{Symbol: "BTCUSDT", Open: 22799.94000000, Close: 22804.36000000, High: 22771.32000000, Low: 22781.59000000},
		{Symbol: "BTCUSDT", Open: 12781.59000000, Close: 12791.19000000, High: 12774.22000000, Low: 12779.73000000},
		{Symbol: "BTCUSDT", Open: 16779.73000000, Close: 16801.10000000, High: 16779.73000000, Low: 16797.62000000},
		{Symbol: "BTCUSDT", Open: 18796.63000000, Close: 18814.71000000, High: 18794.17000000, Low: 18813.10000000},
		{Symbol: "BTCUSDT", Open: 15812.58000000, Close: 15815.45000000, High: 15801.33000000, Low: 15801.97000000},
		{Symbol: "BTCUSDT", Open: 20803.00000000, Close: 20810.97000000, High: 20793.00000000, Low: 20806.12000000},
		{Symbol: "BTCUSDT", Open: 21806.56000000, Close: 21813.35000000, High: 21794.72000000, Low: 21800.85000000},
		{Symbol: "BTCUSDT", Open: 19800.85000000, Close: 19824.15000000, High: 19795.07000000, Low: 19818.93000000},
		{Symbol: "BTCUSDT", Open: 17818.93000000, Close: 17826.82000000, High: 17807.24000000, Low: 17809.98000000},
		{Symbol: "BTCUSDT", Open: 22809.60000000, Close: 22819.73000000, High: 22798.50000000, Low: 22801.94000000},
		{Symbol: "ETHUSDT", Open: 3274.94000000, Close: 3285.19000000, High: 3270.00000000, Low: 3277.96000000},
		{Symbol: "ETHUSDT", Open: 4279.96000000, Close: 4277.96000000, High: 4277.18000000, Low: 4274.04000000},
		{Symbol: "ETHUSDT", Open: 2299.19000000, Close: 2280.67000000, High: 2276.72000000, Low: 2270.09000000},
		{Symbol: "ETHUSDT", Open: 1270.08000000, Close: 1283.52000000, High: 1276.41000000, Low: 1271.79000000},
		{Symbol: "ETHUSDT", Open: 3279.79000000, Close: 3274.22000000, High: 3278.80000000, Low: 3274.68000000},
		{Symbol: "ETHUSDT", Open: 2274.68000000, Close: 2278.00000000, High: 2274.72000000, Low: 2279.19000000},
		{Symbol: "ETHUSDT", Open: 5279.71000000, Close: 5270.94000000, High: 5279.62000000, Low: 5276.20000000},
		{Symbol: "ETHUSDT", Open: 6276.06000000, Close: 6274.66000000, High: 6270.00000000, Low: 6273.07000000},
		{Symbol: "ETHUSDT", Open: 9275.08000000, Close: 9271.13000000, High: 9271.98000000, Low: 9276.70000000},
		{Symbol: "ETHUSDT", Open: 1276.14000000, Close: 1277.19000000, High: 1279.03000000, Low: 1277.36000000},
	}

	timeNow := time.Now()
	// Open := 22799.94000000
	// Close := 22804.36000000
	// High := 22771.32000000
	// Low := 22781.59000000
	for _, data := range assets {
		timeNow = timeNow.Add(time.Duration(time.Minute) * -1)
		// Open += 100
		// Close += 100
		// High += 100
		// Low += 100
		data.Time = timeNow
		// data.Open = Open
		// data.Close = Close
		// data.High = High
		// data.Low = Low
		write_coin_event_with_fluent_Style(client, data)
		time.Sleep(time.Millisecond * 3000)
	}

	results := read_coin_events_as_query_table_result(client)
	// convert results to array to compare with data
	resultsArr := []BinanceAsset{}
	for _, v := range results {
		resultsArr = append(resultsArr, v)
	}

	client.Close()
}
