package server

import (
	"testing"
)

// Ensure that we can query the server for a count of events.
func TestServerSimpleCountQuery(t *testing.T) {
	runTestServer(func(s *Server) {
		setupTestTable("foo")
		setupTestProperty("foo", "fruit", true, "string")
		setupTestProperty("foo", "num", true, "integer")
		setupTestData(t, "foo", [][]string{
			[]string{"a0", "2012-01-01T00:00:00Z", `{"data":{"fruit":"apple"}}`},
			[]string{"a1", "2012-01-01T00:00:00Z", `{"data":{"fruit":"grape"}}`},
			[]string{"a1", "2012-01-01T00:00:01Z", `{"data":{"num":12}}`},
			[]string{"a2", "2012-01-01T00:00:00Z", `{"data":{"fruit":"orange"}}`},
			[]string{"a3", "2012-01-01T00:00:00Z", `{"data":{"fruit":"apple"}}`},
		})

		setupTestTable("bar")
		setupTestProperty("bar", "fruit", true, "string")
		setupTestData(t, "bar", [][]string{
			[]string{"a0", "2012-01-01T00:00:00Z", `{"data":{"fruit":"grape"}}`},
		})

		// Run query.
		query := `{
			"steps":[
				{"type":"selection","dimensions":[],"fields":[{"name":"count","expression":"count()"}]}
			]
		}`
		resp, _ := sendTestHttpRequest("POST", "http://localhost:8800/tables/foo/query", "application/json", query)
		assertResponse(t, resp, 200, `{"count":5}`+"\n", "POST /tables/:name/query failed.")
		resp, _ = sendTestHttpRequest("POST", "http://localhost:8800/tables/bar/query", "application/json", query)
		assertResponse(t, resp, 200, `{"count":1}`+"\n", "POST /tables/:name/query failed.")
	})
}

// Ensure that we can query the server for a count of events with a single dimension.
func TestServerOneDimensionCountQuery(t *testing.T) {
	runTestServer(func(s *Server) {
		setupTestTable("foo")
		setupTestProperty("foo", "fruit", true, "string")
		setupTestProperty("foo", "num", true, "integer")
		setupTestData(t, "foo", [][]string{
			[]string{"b0", "2012-01-01T00:00:00Z", `{"data":{"fruit":"apple"}}`},
			[]string{"b1", "2012-01-01T00:00:00Z", `{"data":{"fruit":"grape"}}`},
			[]string{"b1", "2012-01-01T00:00:01Z", `{"data":{"num":12}}`},
			[]string{"b2", "2012-01-01T00:00:00Z", `{"data":{"fruit":"orange"}}`},
			[]string{"b3", "2012-01-01T00:00:00Z", `{"data":{"fruit":"apple"}}`},
		})

		// Run query.
		query := `{
			"steps":[
				{"type":"selection","dimensions":["fruit"],"fields":[{"name":"count","expression":"count()"}]}
			]
		}`
		//_codegen(t, "foo", query)
		resp, _ := sendTestHttpRequest("POST", "http://localhost:8800/tables/foo/query", "application/json", query)
		assertResponse(t, resp, 200, `{"fruit":{"":{"count":1},"apple":{"count":2},"grape":{"count":1},"orange":{"count":1}}}`+"\n", "POST /tables/:name/query failed.")
	})
}

// Ensure that we can query the server for multiple selections with multiple dimensions.
func TestServerMultiDimensionalQuery(t *testing.T) {
	runTestServer(func(s *Server) {
		setupTestTable("foo")
		setupTestProperty("foo", "gender", false, "string")
		setupTestProperty("foo", "state", false, "factor")
		setupTestProperty("foo", "price", true, "float")
		setupTestProperty("foo", "num", true, "integer")
		setupTestData(t, "foo", [][]string{
			[]string{"c0", "2012-01-01T00:00:00Z", `{"data":{"gender":"m", "state":"NY", "price":100}}`},
			[]string{"c0", "2012-01-01T00:00:01Z", `{"data":{"price":200}}`},
			[]string{"c0", "2012-01-01T00:00:02Z", `{"data":{"state":"CA","price":10}}`},

			[]string{"c1", "2012-01-01T00:00:00Z", `{"data":{"gender":"m", "state":"CA", "price":20}}`},
			[]string{"c1", "2012-01-01T00:00:01Z", `{"data":{"num":1000}}`},

			[]string{"c2", "2012-01-01T00:00:00Z", `{"data":{"gender":"f", "state":"NY", "price":30}}`},
		})

		// Run query.
		query := `{
			"steps":[
				{"type":"selection","name":"s1","dimensions":["gender","state"],"fields":[
					{"name":"count","expression":"count()"},
					{"name":"sum","expression":"sum(price)"}
				]},
				{"type":"selection","dimensions":["gender","state"],"fields":[
					{"name":"minimum","expression":"min(price)"},
					{"name":"maximum","expression":"max(price)"}
				]}
			]
		}`
		//_codegen(t, "foo", query)
		resp, _ := sendTestHttpRequest("POST", "http://localhost:8800/tables/foo/query", "application/json", query)
		assertResponse(t, resp, 200, `{"gender":{"f":{"state":{"NY":{"maximum":30,"minimum":30}}},"m":{"state":{"CA":{"maximum":20,"minimum":0},"NY":{"maximum":200,"minimum":100}}}},"s1":{"gender":{"f":{"state":{"NY":{"count":1,"sum":30}}},"m":{"state":{"CA":{"count":3,"sum":30},"NY":{"count":2,"sum":300}}}}}}`+"\n", "POST /tables/:name/query failed.")
	})
}

// Ensure that we can perform a non-sessionized funnel analysis.
func TestServerFunnelAnalysisQuery(t *testing.T) {
	runTestServer(func(s *Server) {
		setupTestTable("foo")
		setupTestProperty("foo", "action", true, "factor")
		setupTestData(t, "foo", [][]string{
			// A0[0..0]..A1[1..2] occurs twice for this object.
			[]string{"d0", "2012-01-01T00:00:00Z", `{"data":{"action":"A0"}}`},
			[]string{"d0", "2012-01-01T00:00:01Z", `{"data":{"action":"A1"}}`},
			[]string{"d0", "2012-01-01T00:00:02Z", `{"data":{"action":"A2"}}`},
			[]string{"d0", "2012-01-01T12:00:00Z", `{"data":{"action":"A0"}}`},
			[]string{"d0", "2012-01-01T13:00:00Z", `{"data":{"action":"A0"}}`},
			[]string{"d0", "2012-01-01T14:00:00Z", `{"data":{"action":"A1"}}`},

			// A0[0..0]..A1[1..2] occurs once for this object. (Second time matches A1[1..3]).
			[]string{"e1", "2012-01-01T00:00:00Z", `{"data":{"action":"A0"}}`},
			[]string{"e1", "2012-01-01T00:00:01Z", `{"data":{"action":"A0"}}`},
			[]string{"e1", "2012-01-01T00:00:02Z", `{"data":{"action":"A1"}}`},
			[]string{"e1", "2012-01-02T00:00:00Z", `{"data":{"action":"A0"}}`},
			[]string{"e1", "2012-01-02T00:00:01Z", `{"data":{"action":"A0"}}`},
			[]string{"e1", "2012-01-02T00:00:02Z", `{"data":{"action":"A0"}}`},
			[]string{"e1", "2012-01-02T00:00:03Z", `{"data":{"action":"A1"}}`},
		})

		// Run query.
		query := `{
			"steps":[
				{"type":"condition","expression":"action == 'A0'","steps":[
					{"type":"condition","expression":"action == 'A1'","within":[1,2],"steps":[
						{"type":"selection","dimensions":["action"],"fields":[{"name":"count","expression":"count()"}]}
					]}
				]}
			]
		}`
		resp, _ := sendTestHttpRequest("POST", "http://localhost:8800/tables/foo/query", "application/json", query)
		assertResponse(t, resp, 200, `{"action":{"A1":{"count":3}}}`+"\n", "POST /tables/:name/query failed.")
	})
}

// Ensure that we can factorize overlapping factors.
func TestServerFactorizeOverlappingQueries(t *testing.T) {
	runTestServer(func(s *Server) {
		setupTestTable("foo")
		setupTestProperty("foo", "action", false, "factor")
		setupTestData(t, "foo", [][]string{
			[]string{"f0", "2012-01-01T00:00:00Z", `{"data":{"action":"A0"}}`},
		})

		// Run query.
		query := `{
			"steps":[
				{"type":"selection","name":"q","dimensions":["action"],"fields":[{"name":"count1","expression":"count()"}]},
				{"type":"selection","name":"q","dimensions":["action"],"fields":[{"name":"count2","expression":"count()"}]}
			]
		}`
		//_codegen(t, "foo", query)
		resp, _ := sendTestHttpRequest("POST", "http://localhost:8586/tables/foo/query", "application/json", query)
		assertResponse(t, resp, 200, `{"q":{"action":{"A0":{"count1":1,"count2":1}}}}`+"\n", "POST /tables/:name/query failed.")
	})
}

// Ensure that we can perform a sessionized funnel analysis.
func TestServerSessionizedFunnelAnalysisQuery(t *testing.T) {
	runTestServer(func(s *Server) {
		setupTestTable("foo")
		setupTestProperty("foo", "action", false, "factor")
		setupTestData(t, "foo", [][]string{
			// A0[0..0]..A1[1..1] occurs once for this object. The second one is broken across sessions.
			[]string{"f0", "2012-01-01T00:00:00Z", `{"data":{"action":"A0"}}`},
			[]string{"f0", "2012-01-01T01:59:59Z", `{"data":{"action":"A1"}}`},
			[]string{"f0", "2012-01-02T00:00:00Z", `{"data":{"action":"A0"}}`},
			[]string{"f0", "2012-01-02T02:00:00Z", `{"data":{"action":"A1"}}`},
		})

		// Run query.
		query := `{
			"sessionIdleTime":7200,
			"steps":[
				{"type":"condition","expression":"action == 'A0'","steps":[
					{"type":"condition","expression":"action == 'A1'","within":[1,1],"steps":[
						{"type":"selection","dimensions":["action"],"fields":[{"name":"count","expression":"count()"}]}
					]}
				]}
			]
		}`
		//_codegen(t, "foo", query)
		resp, _ := sendTestHttpRequest("POST", "http://localhost:8800/tables/foo/query", "application/json", query)
		assertResponse(t, resp, 200, `{"action":{"A1":{"count":1}}}`+"\n", "POST /tables/:name/query failed.")
	})
}

// Ensure that we can utilitize the timestamp in the query.
func TestServerTimestampQuery(t *testing.T) {
	runTestServer(func(s *Server) {
		setupTestTable("foo")
		setupTestProperty("foo", "action", true, "factor")
		setupTestData(t, "foo", [][]string{
			[]string{"00", "1970-01-01T00:00:00Z", `{"data":{"action":"A0"}}`},
			[]string{"00", "1970-01-01T00:00:02Z", `{"data":{"action":"A1"}}`},
			[]string{"00", "1970-01-01T00:00:04Z", `{"data":{"action":"A2"}}`},
			[]string{"00", "1970-01-01T00:00:06Z", `{"data":{"action":"A3"}}`},
			[]string{"00", "1970-01-01T00:01:00Z", `{"data":{"action":"A4"}}`},
			[]string{"01", "1970-01-01T00:00:02Z", `{"data":{"action":"A5"}}`},
			[]string{"02", "1970-01-01T00:00:02Z", `{"data":{"action":"A5"}}`},
		})

		// Run query.
		query := `{
			"steps":[
				{"type":"condition","expression":"timestamp >= 2 && timestamp < 6","steps":[
					{"type":"selection","dimensions":["action"],"fields":[
						{"name":"count","expression":"count()"},
						{"name":"tsSum","expression":"sum(timestamp)"}
					]}
				]}
			]
		}`
		resp, _ := sendTestHttpRequest("POST", "http://localhost:8800/tables/foo/query", "application/json", query)
		assertResponse(t, resp, 200, `{"action":{"A1":{"count":1,"tsSum":2},"A2":{"count":1,"tsSum":4},"A5":{"count":2,"tsSum":4}}}`+"\n", "POST /tables/:name/query failed.")
	})
}

// Ensure that we can query the server for a histogram of values.
func TestServerHistogramQuery(t *testing.T) {
	runTestServer(func(s *Server) {
		setupTestTable("foo")
		setupTestProperty("foo", "val", true, "integer")
		setupTestData(t, "foo", [][]string{
			[]string{"00", "2012-01-01T00:00:00Z", `{"data":{"val":3}}`}, // Different servlet.

			[]string{"01", "2012-01-01T00:00:00Z", `{"data":{"val":1}}`},
			[]string{"01", "2012-01-01T00:00:01Z", `{"data":{"val":2}}`},
			[]string{"01", "2012-01-01T00:00:02Z", `{"data":{"val":0}}`},
			[]string{"01", "2012-01-01T00:00:03Z", `{"data":{"val":3}}`},
			[]string{"01", "2012-01-01T00:00:04Z", `{"data":{"val":4}}`},
			[]string{"01", "2012-01-01T00:00:05Z", `{"data":{"val":4}}`},

			[]string{"02", "2012-01-01T00:00:00Z", `{"data":{"val":-1}}`},  // Out of range
			[]string{"02", "2012-01-01T00:00:01Z", `{"data":{"val":100}}`}, // Out of range
		})

		// Run query.
		query := `{
			"steps":[
				{"type":"selection","dimensions":[],"fields":[{"name":"hist","expression":"histogram(val)"}]}
			]
		}`
		resp, _ := sendTestHttpRequest("POST", "http://localhost:8800/tables/foo/query", "application/json", query)
		assertResponse(t, resp, 200, `{"hist":{"__histogram__":true,"bins":{"0":3,"1":1,"2":5},"count":3,"max":4,"min":0,"width":1.3333333333333333}}`+"\n", "POST /tables/:name/query failed.")
	})
}

// Ensure that we can can filter by prefix.
func TestServerPrefixQuery(t *testing.T) {
	runTestServer(func(s *Server) {
		setupTestTable("foo")
		setupTestProperty("foo", "price", true, "integer")
		setupTestData(t, "foo", [][]string{
			[]string{"0010a", "2012-01-01T00:00:00Z", `{"data":{"price":100}}`},
			[]string{"0010b", "2012-01-01T00:00:00Z", `{"data":{"price":200}}`},
			[]string{"0010b", "2012-01-01T00:00:01Z", `{}`},
			[]string{"0020a", "2012-01-01T00:00:00Z", `{"data":{"price":30}}`},
			[]string{"0030a", "2012-01-01T00:00:00Z", `{"data":{"price":40}}`},
		})

		// Run query.
		query := `{
			"prefix":"001",
			"steps":[
				{"type":"selection","dimensions":[],"fields":[{"name":"totalPrice","expression":"sum(price)"}]}
			]
		}`
		resp, _ := sendTestHttpRequest("POST", "http://localhost:8800/tables/foo/query", "application/json", query)
		assertResponse(t, resp, 200, `{"totalPrice":300}`+"\n", "POST /tables/:name/query failed.")
	})
}

// Ensure that we can run basic stats.
func TestServerStatsQuery(t *testing.T) {
	runTestServer(func(s *Server) {
		setupTestTable("foo")
		setupTestProperty("foo", "price", true, "integer")
		setupTestData(t, "foo", [][]string{
			[]string{"0010a", "2012-01-01T00:00:00Z", `{"data":{"price":100}}`},
			[]string{"0010b", "2012-01-01T00:00:00Z", `{"data":{"price":200}}`},
			[]string{"0010b", "2012-01-01T00:00:01Z", `{"data":{"price":0}}`},
			[]string{"0020a", "2012-01-01T00:00:00Z", `{"data":{"price":30}}`},
			[]string{"0030a", "2012-01-01T00:00:00Z", `{"data":{"price":40}}`},
		})

		resp, _ := sendTestHttpRequest("GET", "http://localhost:8800/tables/foo/stats?prefix=001", "application/json", "")
		assertResponse(t, resp, 200, `{"count":3}`+"\n", "POST /tables/:name/query failed.")
	})
}
