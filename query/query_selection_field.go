package query

import (
	"errors"
	"fmt"
	"regexp"
)

//------------------------------------------------------------------------------
//
// Typedefs
//
//------------------------------------------------------------------------------

type QuerySelectionField struct {
	Name       string
	Expression string
}

//------------------------------------------------------------------------------
//
// Constructors
//
//------------------------------------------------------------------------------

// Creates a new selection field.
func NewQuerySelectionField(name string, expression string) *QuerySelectionField {
	return &QuerySelectionField{Name: name, Expression: expression}
}

//------------------------------------------------------------------------------
//
// Methods
//
//------------------------------------------------------------------------------

//--------------------------------------
// Serialization
//--------------------------------------

// Encodes a query selection into an untyped map.
func (f *QuerySelectionField) Serialize() map[string]interface{} {
	obj := map[string]interface{}{
		"name":       f.Name,
		"expression": f.Expression,
	}
	return obj
}

// Decodes a query selection from an untyped map.
func (f *QuerySelectionField) Deserialize(obj map[string]interface{}) error {
	if obj == nil {
		return errors.New("skyd.QuerySelectionField: Unable to deserialize nil.")
	}

	// Deserialize "expression".
	if expression, ok := obj["expression"].(string); ok && len(expression) > 0 {
		f.Expression = expression
	} else {
		return fmt.Errorf("skyd.QuerySelectionField: Invalid expression: %v", obj["expression"])
	}

	// Deserialize "name".
	if name, ok := obj["name"].(string); ok && len(name) > 0 {
		f.Name = name
	} else {
		return fmt.Errorf("skyd.QuerySelectionField: Invalid name: %v", obj["name"])
	}

	return nil
}

//--------------------------------------
// Expression
//--------------------------------------

// Extracts the parts of the expression. Returns the aggregate function name
// and the aggregate field name.
func (f *QuerySelectionField) ExpressionParts() (string, string, error) {
	r, _ := regexp.Compile(`^ *(?:count\(\)|(sum|min|max|histogram)\((\w+)\)|(\w+)) *$`)
	if m := r.FindStringSubmatch(f.Expression); m != nil {
		if len(m[1]) > 0 { // sum()/min()/max()
			switch m[1] {
			case "sum", "min", "max", "histogram":
				return m[1], m[2], nil
			}
			return "", "", fmt.Errorf("skyd.QuerySelectionField: Invalid aggregate function: %q", f.Expression)
		} else if len(m[3]) > 0 { // assignment
			return "", m[3], nil
		} else { // count()
			return "count", "", nil
		}
	}

	return "", "", fmt.Errorf("skyd.QuerySelectionField: Invalid expression: %q", f.Expression)
}

//--------------------------------------
// Code Generation
//--------------------------------------

// Generates Lua code for the expression.
func (f *QuerySelectionField) CodegenExpression(init bool) (string, error) {
	functionName, fieldName, err := f.ExpressionParts()
	if err != nil {
		return "", err
	}

	switch functionName {
	case "sum":
		return fmt.Sprintf("data.%s = (data.%s or 0) + cursor.event:%s()", f.Name, f.Name, fieldName), nil
	case "min":
		return fmt.Sprintf("if(data.%s == nil or data.%s > cursor.event:%s()) then data.%s = cursor.event:%s() end", f.Name, f.Name, fieldName, f.Name, fieldName), nil
	case "max":
		return fmt.Sprintf("if(data.%s == nil or data.%s < cursor.event:%s()) then data.%s = cursor.event:%s() end", f.Name, f.Name, fieldName, f.Name, fieldName), nil
	case "count":
		return fmt.Sprintf("data.%s = (data.%s or 0) + 1", f.Name, f.Name), nil
	case "histogram":
		if init {
			return fmt.Sprintf("if data.%s == nil then data.%s = sky_histogram_new() end table.insert(data.%s.values, cursor.event:%s())", f.Name, f.Name, f.Name, fieldName), nil
		} else {
			return fmt.Sprintf("if data.%s ~= nil then sky_histogram_insert(data.%s, cursor.event:%s()) end", f.Name, f.Name, fieldName), nil
		}
	case "":
		return fmt.Sprintf("data.%s = cursor.event:%s()", f.Name, fieldName), nil
	}

	return "", fmt.Errorf("skyd.QuerySelectionField: Unexpected codegen error: %q", f.Expression)
}

// Generates Lua code for the merge expression.
func (f *QuerySelectionField) CodegenMergeExpression() (string, error) {
	functionName, _, err := f.ExpressionParts()
	if err != nil {
		return "", err
	}

	switch functionName {
	case "sum":
		return fmt.Sprintf("result.%s = (result.%s or 0) + (data.%s or 0)", f.Name, f.Name, f.Name), nil
	case "min":
		return fmt.Sprintf("if(result.%s == nil or result.%s > data.%s) then result.%s = data.%s end", f.Name, f.Name, f.Name, f.Name, f.Name), nil
	case "max":
		return fmt.Sprintf("if(result.%s == nil or result.%s < data.%s) then result.%s = data.%s end", f.Name, f.Name, f.Name, f.Name, f.Name), nil
	case "count":
		return fmt.Sprintf("result.%s = (result.%s or 0) + (data.%s or 0)", f.Name, f.Name, f.Name), nil
	case "histogram":
		return fmt.Sprintf("result.%s = sky_histogram_merge(result.%s, data.%s)", f.Name, f.Name, f.Name), nil
	case "":
		return fmt.Sprintf("result.%s = data.%s", f.Name, f.Name), nil
	}

	return "", fmt.Errorf("skyd.QuerySelectionField: Unexpected merge codegen error: %q", f.Expression)
}

//--------------------------------------
// Initialization
//--------------------------------------

// Checks if the field requires the data structure to be initialized before
// aggregation. This will occur when computing histograms since all servlets
// need to insert into the same bins.
func (f *QuerySelectionField) RequiresInitialization() bool {
	functionName, _, _ := f.ExpressionParts()
	if functionName == "histogram" {
		return true
	}
	return false
}
