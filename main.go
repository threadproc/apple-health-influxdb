package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"io/ioutil"
	"net/http"
	"time"

	"github.com/gorilla/mux"
	influxdb2 "github.com/influxdata/influxdb-client-go/v2"
	influxapi "github.com/influxdata/influxdb-client-go/v2/api"
	log "github.com/sirupsen/logrus"
)

var influxClient influxdb2.Client
var influxWriteAPI influxapi.WriteAPIBlocking
var authenticationToken string

type healthDataPayload struct {
	Data struct {
		Workouts []*healthDataWorkout `json:"workouts"`
		Metrics  []*healthDataMetric  `json:"metrics"`
	}
}

type healthDataWorkout struct{}

type healthDataMetric struct {
	Name  string                   `json:"name"`
	Data  []map[string]interface{} `json:"data"`
	Units string                   `json:"units"`
}

func handleDataPayload(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	if r.Header.Get("Authorization") != authenticationToken {
		w.WriteHeader(http.StatusForbidden)
		w.Write([]byte("missing or invalid Authorization header"))

		log.Warn("Invalid token supplied: ", r.Header.Get("Authorization"), " != ", authenticationToken)

		return
	}

	bdbs, err := ioutil.ReadAll(r.Body)
	if err != nil {
		w.WriteHeader(500)
		log.WithError(err).Error("Failed to read body!")
		return
	}

	// parse the payload
	payload := &healthDataPayload{}
	if err := json.Unmarshal(bdbs, payload); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		log.WithError(err).Error("Failed to parse payload")
		return
	}

	// write the most recent payload to file for analysis if needed
	ioutil.WriteFile("payload.json", bdbs, 0644)

	// parse every metric
	hasErrors := false
	for _, metric := range payload.Data.Metrics {
		if err := incomingMetric(metric); err != nil {
			log.WithError(err).Error("Failed to handle incoming metric: ", metric.Name)
			hasErrors = true
		}
	}
	if hasErrors {
		w.WriteHeader(500)
		return
	}
}

func incomingMetric(metric *healthDataMetric) error {
	// no data, no action
	if len(metric.Data) == 0 {
		return nil
	}

	log.Info("Processing metric ", metric.Name)

	// TODO: process sleep more thoroughly (once I get an Apple Watch I guess)
	if metric.Name == "sleep_analysis" {
		return nil
	}

	for _, datum := range metric.Data {
		if err := parseMetricDataPoint(metric, datum); err != nil {
			return err
		}
	}

	return nil
}

func parseMetricDataPoint(metric *healthDataMetric, datum map[string]interface{}) error {
	// we must have a date
	date, ok := datum["date"]
	if !ok {
		return nil
	}
	// parse the time out
	dateStr, ok := date.(string)
	if !ok {
		return errors.New("date must be string")
	}

	dateTime, err := time.Parse("2006-01-02 15:04:05 -0700", dateStr)
	if err != nil {
		return err
	}

	log.Info(dateTime)

	p := influxdb2.NewPointWithMeasurement("apple_health_"+metric.Name).SetTime(dateTime).AddTag("units", metric.Units)
	for k, v := range datum {
		if k == "date" {
			continue
		}

		p.AddField(k, v)
	}

	return influxWriteAPI.WritePoint(context.Background(), p)
}

func main() {
	// flags
	influxdbFlag := flag.String("influxdb-host", "localhost:8086", "InfluxDB 2 hostname and port")
	influxToken := flag.String("influxdb-token", "", "InfluxDB 2 token")
	influxOrg := flag.String("influxdb-org", "", "InfluxDB 2 organization")
	influxBucket := flag.String("influxdb-bucket", "", "InfluxDB 2 bucket")
	listenAddr := flag.String("listen", "0.0.0.0:8082", "Listen address for HTTP server")
	authToken := flag.String("auth-token", "supersecret", "Required authentication token")
	flag.Parse()

	authenticationToken = *authToken

	if *influxToken == "" {
		log.Fatal("You must supply an InfluxDB 2 token")
	}
	if *influxOrg == "" {
		log.Fatal("You must supply an InfluxDB 2 organization")
	}

	// initialize influxdb2 client
	influxClient = influxdb2.NewClient("http://"+*influxdbFlag, *influxToken)
	influxWriteAPI = influxClient.WriteAPIBlocking(*influxOrg, *influxBucket)

	r := mux.NewRouter()

	r.HandleFunc("/data", handleDataPayload)

	log.Info("Using InfluxDB server at ", *influxdbFlag)
	log.Info("Starting HTTP server on ", *listenAddr)
	log.Fatal(http.ListenAndServe(*listenAddr, r))
}
