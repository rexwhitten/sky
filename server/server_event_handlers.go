package server

import (
	"encoding/json"
	"fmt"
	"github.com/gorilla/mux"
	"github.com/skydb/sky/core"
	"io"
	"net/http"
	"time"
)

func (s *Server) addEventHandlers() {
	s.ApiHandleFunc("/tables/{name}/objects/{objectId}/events", nil, s.getEventsHandler).Methods("GET")
	s.ApiHandleFunc("/tables/{name}/objects/{objectId}/events", nil, s.deleteEventsHandler).Methods("DELETE")

	s.ApiHandleFunc("/tables/{name}/objects/{objectId}/events/{timestamp}", nil, s.getEventHandler).Methods("GET")
	s.ApiHandleFunc("/tables/{name}/objects/{objectId}/events/{timestamp}", &InsertEventCommand{}, s.insertEventHandler).Methods("PUT", "PATCH")
	s.ApiHandleFunc("/tables/{name}/objects/{objectId}/events/{timestamp}", &DeleteEventCommand{}, s.deleteEventHandler).Methods("DELETE")

	// Streaming import.
	s.router.HandleFunc("/tables/{name}/events", s.streamUpdateEventsHandler).Methods("PATCH")
}

// GET /tables/:name/objects/:objectId/events
func (s *Server) getEventsHandler(w http.ResponseWriter, req *http.Request, params interface{}) (interface{}, error) {
	vars := mux.Vars(req)
	table, servlet, err := s.GetObjectContext(vars["name"], vars["objectId"])
	if err != nil {
		return nil, err
	}

	// Retrieve raw events.
	events, _, err := servlet.GetEvents(table, vars["objectId"])
	if err != nil {
		return nil, err
	}

	// Denormalize events.
	output := make([]map[string]interface{}, 0)
	for _, event := range events {
		if err = s.fdb.DefactorizeEvent(event, table.Name, table.PropertyFile()); err != nil {
			return nil, err
		}
		e, err := table.SerializeEvent(event)
		if err != nil {
			return nil, err
		}
		output = append(output, e)
	}

	return output, nil
}

// DELETE /tables/:name/objects/:objectId/events
func (s *Server) deleteEventsHandler(w http.ResponseWriter, req *http.Request, params interface{}) (ret interface{}, err error) {
	vars := mux.Vars(req)
	table, servlet, err := s.GetObjectContext(vars["name"], vars["objectId"])
	if err != nil {
		return nil, err
	}

	return nil, servlet.DeleteEvents(table, vars["objectId"])
}

// GET /tables/:name/objects/:objectId/events/:timestamp
func (s *Server) getEventHandler(w http.ResponseWriter, req *http.Request, params interface{}) (ret interface{}, err error) {
	vars := mux.Vars(req)
	table, servlet, err := s.GetObjectContext(vars["name"], vars["objectId"])
	if err != nil {
		return nil, err
	}

	// Parse timestamp.
	timestamp, err := time.Parse(time.RFC3339, vars["timestamp"])
	if err != nil {
		return nil, err
	}

	// Find event.
	event, err := servlet.GetEvent(table, vars["objectId"], timestamp)
	if err != nil {
		return nil, err
	} else if event == nil {
		event = core.NewEvent(vars["timestamp"], map[int64]interface{}{})
	}

	// Convert an event to a serializable object.
	if err = s.fdb.DefactorizeEvent(event, table.Name, table.PropertyFile()); err != nil {
		return nil, err
	}
	e, err := table.SerializeEvent(event)
	if err != nil {
		return nil, err
	}
	return e, nil
}

// PUT   /tables/:name/objects/:objectId/events/:timestamp
// PATCH /tables/:name/objects/:objectId/events/:timestamp
func (s *Server) insertEventHandler(w http.ResponseWriter, req *http.Request, params interface{}) (ret interface{}, err error) {
	vars := mux.Vars(req)
	command := params.(*InsertEventCommand)
	command.TableName = vars["name"]
	command.ObjectId = vars["objectId"]
	command.Timestamp, _ = time.Parse(time.RFC3339, vars["timestamp"])
	if req.Method == "PUT" {
		command.Method = ReplaceMethod
	} else {
		command.Method = MergeMethod
	}
	return nil, s.ExecuteGroupCommand(command)
}

// DELETE /tables/:name/objects/:objectId/events/:timestamp
func (s *Server) deleteEventHandler(w http.ResponseWriter, req *http.Request, params interface{}) (ret interface{}, err error) {
	vars := mux.Vars(req)
	command := params.(*DeleteEventCommand)
	command.TableName = vars["name"]
	command.ObjectId = vars["objectId"]
	command.Timestamp, _ = time.Parse(time.RFC3339, vars["timestamp"])
	return nil, s.ExecuteGroupCommand(command)
}

// PATCH /tables/:name/events
func (s *Server) streamUpdateEventsHandler(w http.ResponseWriter, req *http.Request) {
	vars := mux.Vars(req)
	t0 := time.Now()

	events_written := 0
	err := func() error {
		// Stream in JSON event objects.
		decoder := json.NewDecoder(req.Body)
		for {
			// Read in a JSON object.
			command := &InsertEventCommand{TableName:vars["name"]}
			if err := decoder.Decode(&command); err == io.EOF {
				break
			} else if err != nil {
				return fmt.Errorf("Malformed json event: %v", err)
			}

			if err := s.ExecuteGroupCommand(command); err != nil {
				return err
			}

			events_written++
		}
		return nil
	}()

	if err != nil {
		s.logger.Printf("ERR %v", err)
		fmt.Fprintf(w, `{"message":"%v"}`, err)
	}
	s.logger.Printf("%s \"%s %s %s %d events OK\" %0.3f", req.RemoteAddr, req.Method, req.URL.Path, req.Proto, events_written, time.Since(t0).Seconds())
}
