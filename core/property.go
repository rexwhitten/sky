package core

import (
	"fmt"
	"regexp"
)

//------------------------------------------------------------------------------
//
// Variables
//
//------------------------------------------------------------------------------

var validPropertyNameRegex = regexp.MustCompile(`^\w+$`)

//------------------------------------------------------------------------------
//
// Typedefs
//
//------------------------------------------------------------------------------

// A Property is a loose core column on a Table.
type Property struct {
	Id        int64  `json:"id"`
	Name      string `json:"name"`
	Transient bool   `json:"transient"`
	DataType  string `json:"dataType"`
}

//------------------------------------------------------------------------------
//
// Constructor
//
//------------------------------------------------------------------------------

// NewProperty returns a new Property.
func NewProperty(id int64, name string, transient bool, dataType string) (*Property, error) {
	// Validate name.
	if name == "" {
		return nil, fmt.Errorf("Property name cannot be blank")
	} else if !validPropertyNameRegex.MatchString(name) {
		return nil, fmt.Errorf("Property name contains invalid characters: %s", name)
	}

	// Validate data type.
	switch dataType {
	case FactorDataType, StringDataType, IntegerDataType, FloatDataType, BooleanDataType:
	default:
		return nil, fmt.Errorf("Invalid property data type: %v", dataType)
	}

	return &Property{
		Id:        id,
		Name:      name,
		Transient: transient,
		DataType:  dataType,
	}, nil
}

//------------------------------------------------------------------------------
//
// Methods
//
//------------------------------------------------------------------------------

// Casts a value into this property's data type.
func (p *Property) Cast(value interface{}) interface{} {
	value = normalize(value)
	switch p.DataType {
	case FactorDataType, StringDataType:
		if str, ok := value.(string); ok {
			return str
		} else {
			return ""
		}

	case IntegerDataType:
		if intValue, ok := value.(int64); ok {
			return intValue
		} else if floatValue, ok := value.(float64); ok {
			return int64(floatValue)
		} else {
			return int64(0)
		}

	case FloatDataType:
		if floatValue, ok := value.(float64); ok {
			return floatValue
		} else if intValue, ok := value.(int64); ok {
			return float64(intValue)
		} else {
			return float64(0)
		}

	case BooleanDataType:
		if boolValue, ok := value.(bool); ok {
			return boolValue
		} else {
			return false
		}
	}
	return value
}
