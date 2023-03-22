package main

import (
	"context"
	"testing"

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
			// TODO Validate the data
		})
	}
}
