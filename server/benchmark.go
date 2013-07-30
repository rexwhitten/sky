package server

import (
	"encoding/json"
	"fmt"
	"github.com/skydb/sky/core"
	"io"
	"log"
	"math/rand"
	"net/http"
	"time"
)

var PROPERTIES = []core.Property{
	{0, "first_name", false, "string"},
	{0, "last_name", false, "string"},
	{0, "email", false, "string"},
	{0, "customer_id", false, "integer"},
	{0, "shop_id", false, "integer"},
	{0, "order_id", true, "integer"},
	{0, "payment_type", true, "factor"},
	{0, "financial_status", true, "factor"},
	{0, "country", true, "factor"},
	{0, "province", true, "factor"},
	{0, "city", true, "factor"},
	{0, "subtotal_price", true, "integer"},
	{0, "referrer", true, "string"},
	{0, "location_id", true, "factor"},
	{0, "employee_id", true, "factor"},
	{0, "year", true, "integer"},
	{0, "month", true, "integer"},
	{0, "day", true, "integer"}}

func benchmarkEventsCount(s *Server) int {
	resp, err := sendTestHttpRequest("GET", "http://localhost:8800/tables/benchmark/stats", "application/json", "")
	defer resp.Body.Close()
	if err != nil || resp.StatusCode != 200 {
		return 0
	}
	result := make(map[string]interface{})
	if err = json.NewDecoder(resp.Body).Decode(&result); err != nil {
		log.Fatalf("error 2: %v", err)
		return 0
	}
	count, _ := result["count"].(float64)
	return int(count)
}

func randomString(l int) string {
	bytes := make([]byte, l)
	for i := 0; i < l; i++ {
		bytes[i] = byte(65 + rand.Intn(25))
	}
	return string(bytes)
}

func randomValueForProperty(p *core.Property) interface{} {
	switch p.DataType {
	case core.StringDataType, core.FactorDataType:
		return randomString(1 + rand.Int()%10)
	case core.IntegerDataType:
		return rand.Int() % 10000000
	case core.FloatDataType:
		return rand.Float64()
	}
	return ""
}

func generateBenchmarkData(s *Server, count int, batchSize int) {
	if benchmarkEventsCount(s) != count {
		setupTestTable("benchmark")

		for _, prop := range PROPERTIES {
			setupTestProperty("benchmark", prop.Name, prop.Transient, prop.DataType)
		}

		for batchIndex := 0; batchIndex < count/batchSize; batchIndex++ {
			reader, writer := io.Pipe()

			client := &http.Client{Transport: &http.Transport{DisableKeepAlives: true}}
			req, _ := http.NewRequest("PATCH", "http://localhost:8800/tables/benchmark/events", reader)
			req.Header.Add("Content-Type", "application/json")

			finished := make(chan *http.Response)
			go func() {
				resp, err := client.Do(req)
				if err != nil {
					log.Fatalf("Failure streaming request: %v", err)
				}
				finished <- resp
			}()

			j := json.NewEncoder(writer)
			rand.Seed(time.Now().UnixNano())

			for i := 0; i < batchSize; i++ {
				index := (batchIndex * batchSize) + i
				event := make(map[string]interface{})
				event["timestamp"] = "2012-01-01T03:00:00Z"
				data := make(map[string]interface{})
				event["id"] = fmt.Sprintf("%d", index)
				for _, prop := range PROPERTIES {
					if rand.Float32() > 0.5 {
						data[prop.Name] = randomValueForProperty(&prop)
					}
				}
				event["data"] = data
				err := j.Encode(event)
				if err != nil {
					log.Printf("JSON encoding error: %v", err)
				}
			}

			writer.Close()
			resp := <-finished
			reader.Close()

			if resp.StatusCode != 200 {
				log.Printf("Request failed! %v", req)
			}
		}
	}
}

func withBenchmarkData(path string, events int, batchSize int, f func(s *Server)) {
	runTestServerAt(path, func(s *Server) {
		generateBenchmarkData(s, events, batchSize)
		f(s)
	})
}
