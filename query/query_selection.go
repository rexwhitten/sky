package query

import (
	"bytes"
	"errors"
	"fmt"
	"github.com/skydb/sky/core"
)

//------------------------------------------------------------------------------
//
// Typedefs
//
//------------------------------------------------------------------------------

// A selection step aggregates data in a query.
type QuerySelection struct {
	query      *Query
	id         int
	Name       string
	Dimensions []string
	Fields     []*QuerySelectionField
}

//------------------------------------------------------------------------------
//
// Constructors
//
//------------------------------------------------------------------------------

// Creates a new selection.
func NewQuerySelection(query *Query) *QuerySelection {
	return &QuerySelection{
		query: query,
		id:    query.NextIdentifier(),
	}
}

//------------------------------------------------------------------------------
//
// Accessors
//
//------------------------------------------------------------------------------

// Retrieves the query this selection is associated with.
func (s *QuerySelection) Query() *Query {
	return s.query
}

// Retrieves the function name used during codegen.
func (s *QuerySelection) FunctionName(init bool) string {
	if init {
		return fmt.Sprintf("i%d", s.id)
	}
	return fmt.Sprintf("a%d", s.id)
}

// Retrieves the merge function name used during codegen.
func (s *QuerySelection) MergeFunctionName() string {
	return fmt.Sprintf("m%d", s.id)
}

// Retrieves the child steps.
func (s *QuerySelection) GetSteps() QueryStepList {
	return []QueryStep{}
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
func (s *QuerySelection) Serialize() map[string]interface{} {
	fields := []interface{}{}
	for _, field := range s.Fields {
		fields = append(fields, field.Serialize())
	}

	obj := map[string]interface{}{
		"type":       QueryStepTypeSelection,
		"name":       s.Name,
		"dimensions": s.Dimensions,
		"fields":     fields,
	}
	return obj
}

// Decodes a query selection from an untyped map.
func (s *QuerySelection) Deserialize(obj map[string]interface{}) error {
	if obj == nil {
		return errors.New("skyd.QuerySelection: Unable to deserialize nil.")
	}
	if obj["type"] != QueryStepTypeSelection {
		return fmt.Errorf("skyd.QuerySelection: Invalid step type: %v", obj["type"])
	}

	// Deserialize "name".
	if name, ok := obj["name"].(string); ok {
		s.Name = name
	} else if obj["name"] == nil {
		s.Name = ""
	} else {
		return fmt.Errorf("skyd.QuerySelection: Invalid name: %v", obj["name"])
	}

	// Deserialize "dimensions".
	if dimensions, ok := obj["dimensions"].([]interface{}); ok {
		s.Dimensions = []string{}
		for _, dimension := range dimensions {
			if str, ok := dimension.(string); ok {
				s.Dimensions = append(s.Dimensions, str)
			} else {
				return fmt.Errorf("skyd.QuerySelection: Invalid dimension: %v", dimension)
			}
		}
	} else {
		if obj["dimension"] == nil {
			s.Dimensions = []string{}
		} else {
			return fmt.Errorf("skyd.QuerySelection: Invalid dimensions: %v", obj["dimensions"])
		}
	}

	// Deserialize "fields".
	if fields, ok := obj["fields"].([]interface{}); ok {
		s.Fields = []*QuerySelectionField{}
		for _, field := range fields {
			if fieldMap, ok := field.(map[string]interface{}); ok {
				f := NewQuerySelectionField("", "")
				f.Deserialize(fieldMap)
				s.Fields = append(s.Fields, f)
			} else {
				return fmt.Errorf("skyd.QuerySelection: Invalid field: %v", field)
			}
		}
	} else {
		if obj["field"] == nil {
			s.Fields = []*QuerySelectionField{}
		} else {
			return fmt.Errorf("skyd.QuerySelection: Invalid fields: %v", obj["fields"])
		}
	}

	return nil
}

//--------------------------------------
// Code Generation
//--------------------------------------

// Generates Lua code for the selection aggregation.
func (s *QuerySelection) CodegenAggregateFunction(init bool) (string, error) {
	buffer := new(bytes.Buffer)

	// Generate main function.
	fmt.Fprintf(buffer, "function %s(cursor, data)\n", s.FunctionName(init))

	// Add selection name.
	if s.Name != "" {
		fmt.Fprintf(buffer, "  if data[\"%s\"] == nil then data[\"%s\"] = {} end\n", s.Name, s.Name)
		fmt.Fprintf(buffer, "  data = data[\"%s\"]\n\n", s.Name)
	}

	// Group by dimension.
	for _, dimension := range s.Dimensions {
		fmt.Fprintf(buffer, "  dimension = cursor.event:%s()\n", dimension)
		fmt.Fprintf(buffer, "  if data.%s == nil then data.%s = {} end\n", dimension, dimension)
		fmt.Fprintf(buffer, "  if data.%s[dimension] == nil then data.%s[dimension] = {} end\n", dimension, dimension)
		fmt.Fprintf(buffer, "  data = data.%s[dimension]\n\n", dimension)
	}

	// Select fields.
	for _, field := range s.Fields {
		exp, err := field.CodegenExpression(init)
		if err != nil {
			return "", err
		}
		fmt.Fprintln(buffer, "  "+exp)
	}

	// End function definition.
	fmt.Fprintln(buffer, "end")

	return buffer.String(), nil
}

// Generates Lua code for the selection merge.
func (s *QuerySelection) CodegenMergeFunction() (string, error) {
	buffer := new(bytes.Buffer)

	// Generate nested functions first.
	code, err := s.CodegenInnerMergeFunction(0)
	if err != nil {
		return "", err
	}
	buffer.WriteString(code + "\n")

	// Generate main function.
	fmt.Fprintf(buffer, "function %s(result, data)\n", s.MergeFunctionName())
	if s.Name != "" {
		fmt.Fprintf(buffer, "  if result[\"%s\"] == nil then result[\"%s\"] = {} end\n", s.Name, s.Name)
		fmt.Fprintf(buffer, "  if data[\"%s\"] == nil then data[\"%s\"] = {} end\n", s.Name, s.Name)
		fmt.Fprintf(buffer, "  %sn0(result[\"%s\"], data[\"%s\"])\n", s.MergeFunctionName(), s.Name, s.Name)
	} else {
		fmt.Fprintf(buffer, "  %sn0(result, data)\n", s.MergeFunctionName())
	}
	fmt.Fprintf(buffer, "end\n")

	return buffer.String(), nil
}

// Generates Lua code for the inner merge.
func (s *QuerySelection) CodegenInnerMergeFunction(index int) (string, error) {
	buffer := new(bytes.Buffer)

	// Generate next nested function first.
	if index < len(s.Dimensions) {
		code, err := s.CodegenInnerMergeFunction(index + 1)
		if err != nil {
			return "", err
		}
		buffer.WriteString(code + "\n")
	}

	// Generate a rollup if our index points at a dimension. Otherwise generate
	// the leaf merge.
	fmt.Fprintf(buffer, "function %sn%d(result, data)\n", s.MergeFunctionName(), index)
	if index < len(s.Dimensions) {
		dimension := s.Dimensions[index]
		fmt.Fprintf(buffer, "  if data ~= nil and data.%s ~= nil then\n", dimension)
		fmt.Fprintf(buffer, "    if result.%s == nil then result.%s = {} end\n", dimension, dimension)
		fmt.Fprintf(buffer, "    for k,v in pairs(data.%s) do\n", dimension)
		fmt.Fprintf(buffer, "      if result.%s[k] == nil then result.%s[k] = {} end\n", dimension, dimension)
		fmt.Fprintf(buffer, "      %sn%d(result.%s[k], v)\n", s.MergeFunctionName(), (index + 1), dimension)
		fmt.Fprintf(buffer, "    end\n")
		fmt.Fprintf(buffer, "  end\n")
	} else {
		// Merge fields.
		for _, field := range s.Fields {
			exp, err := field.CodegenMergeExpression()
			if err != nil {
				return "", err
			}
			fmt.Fprintln(buffer, "  "+exp)
		}
	}
	fmt.Fprintf(buffer, "end\n")

	return buffer.String(), nil
}

//--------------------------------------
// Factorization
//--------------------------------------

// Converts factorized fields back to their original strings.
func (s *QuerySelection) Defactorize(data interface{}) error {
	if m, ok := data.(map[interface{}]interface{}); ok {
		// If this is a named selection then drill in first.
		if s.Name != "" {
			if m2, ok := m[s.Name].(map[interface{}]interface{}); ok {
				m = m2
			} else {
				return nil
			}
		}

		// Recursively defactorize dimensions and then fields.
		return s.defactorize(m, 0)
	}

	return nil
}

// Recursively defactorizes dimensions.
func (s *QuerySelection) defactorize(data interface{}, index int) error {
	if index >= len(s.Dimensions) {
		return nil
	}
	// Ignore any values that are nil or not maps.
	inner, ok := data.(map[interface{}]interface{})
	if !ok || data == nil {
		return nil
	}

	// Retrieve property.
	dimension := s.Dimensions[index]
	property := s.query.table.PropertyFile().GetPropertyByName(dimension)
	if property == nil {
		return fmt.Errorf("skyd.QuerySelection: Property not found: %s", dimension)
	}

	// Defactorize.
	if outer, ok := inner[dimension].(map[interface{}]interface{}); ok {
		copy := map[interface{}]interface{}{}
		for k, v := range outer {
			if property.DataType == core.FactorDataType {
				// Only process this if it hasn't been defactorized already. Duplicate
				// defactorization can occur if there are multiple overlapping selections.
				if sequence, ok := normalize(k).(int64); ok {
					stringValue, err := s.query.fdb.Defactorize(s.query.table.Name, dimension, uint64(sequence))
					if err != nil {
						return err
					}
					copy[stringValue] = v
				} else {
					copy[k] = v
				}
			} else {
				copy[k] = v
			}

			// Defactorize next dimension.
			s.defactorize(v, index+1)
		}
		inner[dimension] = copy
	}

	return nil
}

//--------------------------------------
// Initialization
//--------------------------------------

// Checks if any of the selection fields require initialization before
// performing aggregation.
func (s *QuerySelection) RequiresInitialization() bool {
	for _, field := range s.Fields {
		if field.RequiresInitialization() {
			return true
		}
	}
	return false
}
