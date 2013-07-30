package server

import (
	"github.com/gorilla/mux"
	"github.com/skydb/sky/query"
	"net/http"
)

func (s *Server) addQueryHandlers() {
	s.ApiHandleFunc("/tables/{name}/stats", nil, s.statsHandler).Methods("GET")
	s.ApiHandleFunc("/tables/{name}/query", nil, s.queryHandler).Methods("POST")
	s.ApiHandleFunc("/tables/{name}/query/codegen", nil, s.queryCodegenHandler).Methods("POST")
}

// GET /tables/:name/stats
func (s *Server) statsHandler(w http.ResponseWriter, req *http.Request, params interface{}) (interface{}, error) {
	vars := mux.Vars(req)

	// Return an error if the table already exists.
	table, err := s.OpenTable(vars["name"])
	if err != nil {
		return nil, err
	}

	// Run a simple count query.
	q := query.NewQuery(table, s.fdb)
	selection := query.NewQuerySelection(q)
	selection.Fields = append(selection.Fields, query.NewQuerySelectionField("count", "count()"))
	q.Prefix = req.FormValue("prefix")
	q.Steps = append(q.Steps, selection)

	return s.RunQuery(table, q)
}

// POST /tables/:name/query
func (s *Server) queryHandler(w http.ResponseWriter, req *http.Request, params interface{}) (interface{}, error) {
	vars := mux.Vars(req)
	p := params.(map[string]interface{})

	// Return an error if the table already exists.
	table, err := s.OpenTable(vars["name"])
	if err != nil {
		return nil, err
	}

	// Deserialize the query.
	query := query.NewQuery(table, s.fdb)
	err = q.Deserialize(p)
	if err != nil {
		return nil, err
	}

	return s.RunQuery(table, q)
}

// POST /tables/:name/query/codegen
func (s *Server) queryCodegenHandler(w http.ResponseWriter, req *http.Request, params interface{}) (interface{}, error) {
	vars := mux.Vars(req)
	p := params.(map[string]interface{})

	// Retrieve table and codegen query.
	var source string
	// Return an error if the table already exists.
	table, err := s.OpenTable(vars["name"])
	if err != nil {
		return nil, err
	}

	// Deserialize the query.
	q := NewQuery(table, s.fdb)
	err = query.Deserialize(p)
	if err != nil {
		return nil, err
	}

	// Generate the query source code.
	source, err = q.Codegen()
	//fmt.Println(source)

	return source, &TextPlainContentTypeError{}
}
