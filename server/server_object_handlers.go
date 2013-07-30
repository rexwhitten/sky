package server

import (
	"errors"
	"fmt"
	"github.com/gorilla/mux"
	"net/http"
)

func (s *Server) addObjectHandlers() {
	s.ApiHandleFunc("/tables/{name}/objects/{objectId}/merge", func(w http.ResponseWriter, req *http.Request, params map[string]interface{}) (interface{}, error) {
		return s.mergeObjectHandler(w, req, params)
	}).Methods("POST")
}

// POST /tables/:name/objects/merge
func (s *Server) mergeObjectHandler(w http.ResponseWriter, req *http.Request, params map[string]interface{}) (interface{}, error) {
	vars := mux.Vars(req)

	// Set up object ids.
	destObjectId := vars["objectId"]
	srcObjectId, ok := params["id"].(string)
	if !ok {
		return nil, fmt.Errorf("Invalid source object id: %v", params["id"])
	} else if destObjectId == srcObjectId {
		return nil, errors.New("Cannot merge an object into itself")
	}

	// Retrieve source timeline.
	srcTable, srcServlet, err := s.GetObjectContext(vars["name"], srcObjectId)
	if err != nil {
		return nil, err
	}
	srcEvents, _, err := srcServlet.GetEvents(srcTable, srcObjectId)
	if err != nil {
		return nil, err
	}

	// Insert events into destination object.
	if len(srcEvents) > 0 {
		destTable, destServlet, err := s.GetObjectContext(vars["name"], destObjectId)
		if err != nil {
			return nil, err
		}
		if err = destServlet.PutEvents(destTable, destObjectId, srcEvents, true); err != nil {
			return nil, err
		}

		// Delete source timeline.
		if err = srcServlet.DeleteEvents(srcTable, srcObjectId); err != nil {
			return nil, err
		}
	}

	return nil, nil
}
