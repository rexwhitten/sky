package server

import (
	"github.com/skydb/sky"
	"net/http"
)

func (s *Server) addHandlers() {
	s.ApiHandleFunc("/ping", nil, s.pingHandler).Methods("GET")
	s.ApiHandleFunc("/", nil, s.indexHandler).Methods("GET")
}

// GET /ping
func (s *Server) pingHandler(w http.ResponseWriter, req *http.Request, params interface{}) (interface{}, error) {
	return map[string]interface{}{"message": "ok"}, nil
}

// GET /
func (s *Server) indexHandler(w http.ResponseWriter, req *http.Request, params interface{}) (interface{}, error) {
	return map[string]interface{}{"sky": "welcome", "version": sky.Version}, nil
}
