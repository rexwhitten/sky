package server

import (
	"testing"
)

// Ensure that we can merge one object timeline to another.
func TestServerMergeObject(t *testing.T) {
	runTestServer(func(s *Server) {
		setupTestTable("foo")
		setupTestProperty("foo", "name", false, "factor")
		setupTestProperty("foo", "num", true, "integer")
		setupTestData(t, "foo", [][]string{
			[]string{"a", "2012-01-02T00:00:00Z", `{"data":{"name":"john"}}`},
			[]string{"a", "2012-01-04T00:00:00Z", `{"data":{"num":10}}`},
			[]string{"a", "2012-01-06T00:00:00Z", `{"data":{"name":"susy","num":20}}`},
			[]string{"b", "2012-01-01T00:00:00Z", `{"data":{"name":"john"}}`},
			[]string{"b", "2012-01-03T00:00:00Z", `{"data":{"name":"susy"}}`},
			[]string{"b", "2012-01-07T00:00:00Z", `{"data":{"num":30}}`},
		})

		// Merge the two objects
		resp, _ := sendTestHttpRequest("POST", "http://localhost:8586/tables/foo/objects/a/merge", "application/json", `{"id":"b"}`)
		assertResponse(t, resp, 200, "", "Merge failed")

		// Verify combined timeline.
		exp := `[{"data":{"name":"john"},"timestamp":"2012-01-01T00:00:00Z"},{"data":{"name":"susy"},"timestamp":"2012-01-03T00:00:00Z"},{"data":{"num":10},"timestamp":"2012-01-04T00:00:00Z"},{"data":{"num":20},"timestamp":"2012-01-06T00:00:00Z"},{"data":{"num":30},"timestamp":"2012-01-07T00:00:00Z"}]` + "\n"
		resp, _ = sendTestHttpRequest("GET", "http://localhost:8586/tables/foo/objects/a/events", "application/json", "")
		assertResponse(t, resp, 200, exp, "GET /tables/foo/objects/a/events failed.")

		// Verify source timeline is deleted.
		resp, _ = sendTestHttpRequest("GET", "http://localhost:8586/tables/foo/objects/b/events", "application/json", "")
		assertResponse(t, resp, 200, "[]\n", "GET /tables/foo/objects/a/events failed.")
	})
}

// Ensure a merge from a non-existent source object is ignored.
func TestServerMergeNonExistentSourceObject(t *testing.T) {
	runTestServer(func(s *Server) {
		setupTestTable("foo")
		setupTestProperty("foo", "num", true, "integer")
		setupTestData(t, "foo", [][]string{
			[]string{"a", "2012-01-01T00:00:00Z", `{"data":{"num":10}}`},
		})

		// Merge the two objects
		resp, _ := sendTestHttpRequest("POST", "http://localhost:8586/tables/foo/objects/a/merge", "application/json", `{"id":"b"}`)
		assertResponse(t, resp, 200, "", "Merge failed")
	})
}

// Ensure a merge from a non-existent destination object works.
func TestServerMergeNonExistentDestinationObject(t *testing.T) {
	runTestServer(func(s *Server) {
		setupTestTable("foo")
		setupTestProperty("foo", "num", true, "integer")
		setupTestData(t, "foo", [][]string{
			[]string{"b", "2012-01-01T00:00:00Z", `{"data":{"num":10}}`},
		})

		// Merge the two objects
		resp, _ := sendTestHttpRequest("POST", "http://localhost:8586/tables/foo/objects/a/merge", "application/json", `{"id":"b"}`)
		assertResponse(t, resp, 200, "", "Merge failed")

		// Verify destination timeline.
		resp, _ = sendTestHttpRequest("GET", "http://localhost:8586/tables/foo/objects/a/events", "application/json", "")
		assertResponse(t, resp, 200, `[{"data":{"num":10},"timestamp":"2012-01-01T00:00:00Z"}]`+"\n", "GET /tables/foo/objects/a/events failed.")
	})
}

// Ensure that we cannot merge an object into itself.
func TestServerMergeSameObjectNotAllowed(t *testing.T) {
	runTestServer(func(s *Server) {
		setupTestTable("foo")
		resp, _ := sendTestHttpRequest("POST", "http://localhost:8586/tables/foo/objects/a/merge", "application/json", `{"id":"a"}`)
		assertResponse(t, resp, 500, `{"message":"Cannot merge an object into itself"}`+"\n", "Merge error")
	})
}
