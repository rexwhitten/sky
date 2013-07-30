package server

import (
	"github.com/gorilla/mux"
	"net/http"
)

func (s *Server) addPropertyHandlers() {
	s.ApiHandleFunc("/tables/{name}/properties", nil, s.getPropertiesHandler).Methods("GET")
	s.ApiHandleFunc("/tables/{name}/properties/{propertyName}", nil, s.getPropertyHandler).Methods("GET")
	s.ApiHandleFunc("/tables/{name}/properties", &CreatePropertyCommand{}, s.createPropertyHandler).Methods("POST")
	s.ApiHandleFunc("/tables/{name}/properties/{propertyName}", &UpdatePropertyCommand{}, s.updatePropertyHandler).Methods("PATCH")
	s.ApiHandleFunc("/tables/{name}/properties/{propertyName}", &DeletePropertyCommand{}, s.deletePropertyHandler).Methods("DELETE")
}

// GET /tables/:name/properties
func (s *Server) getPropertiesHandler(w http.ResponseWriter, req *http.Request, params interface{}) (interface{}, error) {
	vars := mux.Vars(req)
	table, err := s.OpenTable(vars["name"])
	if err != nil {
		return nil, err
	}

	return table.GetProperties()
}

// GET /tables/:name/properties/:propertyName
func (s *Server) getPropertyHandler(w http.ResponseWriter, req *http.Request, params interface{}) (interface{}, error) {
	vars := mux.Vars(req)
	table, err := s.OpenTable(vars["name"])
	if err != nil {
		return nil, err
	}

	return table.GetPropertyByName(vars["propertyName"])
}

// POST /tables/:name/properties
func (s *Server) createPropertyHandler(w http.ResponseWriter, req *http.Request, params interface{}) (interface{}, error) {
	vars := mux.Vars(req)
	command := params.(*CreatePropertyCommand)
	command.TableName = vars["name"]
	err := s.ExecuteClusterCommand(command)

	table, err := s.OpenTable(command.TableName)
	if err != nil {
		return nil, err
	}
	property, _ := table.GetPropertyByName(command.Name)
	return property, err
}

// PATCH /tables/:name/properties/:propertyName
func (s *Server) updatePropertyHandler(w http.ResponseWriter, req *http.Request, params interface{}) (interface{}, error) {
	vars := mux.Vars(req)
	command := params.(*UpdatePropertyCommand)
	command.TableName = vars["name"]
	command.OriginalName = vars["propertyName"]
	err := s.ExecuteClusterCommand(command)

	table, err := s.OpenTable(command.TableName)
	if err != nil {
		return nil, err
	}
	property, _ := table.GetPropertyByName(command.Name)
	return property, err
}

// DELETE /tables/:name/properties/:propertyName
func (s *Server) deletePropertyHandler(w http.ResponseWriter, req *http.Request, params interface{}) (interface{}, error) {
	vars := mux.Vars(req)
	command := params.(*DeletePropertyCommand)
	command.TableName = vars["name"]
	command.Name = vars["propertyName"]
	return nil, s.ExecuteClusterCommand(command)
}
