package server

import (
	"github.com/gorilla/mux"
	"github.com/skydb/sky/core"
	"net/http"
)

func (s *Server) addTableHandlers() {
	s.ApiHandleFunc("/tables", nil, s.getTablesHandler).Methods("GET")
	s.ApiHandleFunc("/tables/{name}", nil, s.getTableHandler).Methods("GET")
	s.ApiHandleFunc("/tables", &CreateTableCommand{}, s.createTableHandler).Methods("POST")
	s.ApiHandleFunc("/tables/{name}", nil, s.deleteTableHandler).Methods("DELETE")
}

// GET /tables
func (s *Server) getTablesHandler(w http.ResponseWriter, req *http.Request, params interface{}) (interface{}, error) {
	return s.GetAllTables()
}

// GET /tables/:name
func (s *Server) getTableHandler(w http.ResponseWriter, req *http.Request, params interface{}) (interface{}, error) {
	vars := mux.Vars(req)
	return s.OpenTable(vars["name"])
}

// POST /tables
func (s *Server) createTableHandler(w http.ResponseWriter, req *http.Request, params interface{}) (interface{}, error) {
	command := params.(*CreateTableCommand)
	err := s.ExecuteClusterCommand(command)
	table, _ := s.OpenTable(command.Name)
	return table, err
}

// DELETE /tables/:name
func (s *Server) deleteTableHandler(w http.ResponseWriter, req *http.Request, params interface{}) (interface{}, error) {
	vars := mux.Vars(req)
	command := NewDeleteTableCommand(vars["name"])
	return nil, s.ExecuteClusterCommand(command)
}
