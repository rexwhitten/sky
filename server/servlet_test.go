package server

import (
	"github.com/skydb/sky/core"
	"io/ioutil"
	"os"
	"testing"
)

// Ensure that we can open and close a servlet.
func TestOpen(t *testing.T) {
	path, err := ioutil.TempDir("", "")
	defer os.RemoveAll(path)

	servlet := NewServlet(path, nil)
	defer servlet.Close()
	err = servlet.Open()
	if err != nil {
		t.Fatalf("Unable to open servlet: %v", err)
	}
}

// Ensure that we can add events and read them back.
func TestServletPutEvent(t *testing.T) {
	// Setup blank database.
	path, err := ioutil.TempDir("", "")
	defer os.RemoveAll(path)
	table := core.NewTable("test", "/tmp/test")
	servlet := NewServlet(path, nil)
	defer servlet.Close()
	_ = servlet.Open()

	// Setup source events.
	input := make([]*core.Event, 3)
	input[0] = core.NewEvent("2012-01-02T00:00:00Z", map[int64]interface{}{-1: 20, 1: "foo", 3: "baz"})
	input[1] = core.NewEvent("2012-01-01T00:00:00Z", map[int64]interface{}{-1: 20, 2: "bar", 3: "baz"})
	input[2] = core.NewEvent("2012-01-03T00:00:00Z", map[int64]interface{}{-1: 20, 1: "foo", 3: "baz"})

	// Add events.
	for _, e := range input {
		err = servlet.PutEvent(table, "bob", e, true)
		if err != nil {
			t.Fatalf("Unable to add event: %v", err)
		}
	}

	// Setup expected events.
	expected := make([]*core.Event, len(input))
	expected[0] = core.NewEvent("2012-01-01T00:00:00Z", map[int64]interface{}{-1: 20, 2: "bar", 3: "baz"})
	expected[1] = core.NewEvent("2012-01-02T00:00:00Z", map[int64]interface{}{-1: 20, 1: "foo"})
	expected[2] = core.NewEvent("2012-01-03T00:00:00Z", map[int64]interface{}{-1: 20})
	expectedState := core.NewEvent("2012-01-03T00:00:00Z", map[int64]interface{}{1: "foo", 2: "bar", 3: "baz"})

	// Read events out.
	output, state, err := servlet.GetEvents(table, "bob")
	if err != nil {
		t.Fatalf("Unable to retrieve events: %v", err)
	}
	if !expectedState.Equal(state) {
		t.Fatalf("Incorrect state.\nexp: %v\ngot: %v", expectedState, state)
	}
	if len(output) != len(expected) {
		t.Fatalf("Expected %v events, received %v", len(expected), len(output))
	}
	for i := range output {
		if !expected[i].Equal(output[i]) {
			t.Fatalf("Events not equal (%d):\n  IN:  %v\n  OUT: %v", i, expected[i], output[i])
		}
	}
}
