package query

import (
	"bytes"
	"fmt"
)

//------------------------------------------------------------------------------
//
// Constants
//
//------------------------------------------------------------------------------

const (
	QueryStepTypeCondition = "condition"
	QueryStepTypeSelection = "selection"
)

//------------------------------------------------------------------------------
//
// Typedefs
//
//------------------------------------------------------------------------------

type QueryStep interface {
	FunctionName(init bool) string
	MergeFunctionName() string
	GetSteps() QueryStepList
	Serialize() map[string]interface{}
	Deserialize(map[string]interface{}) error
	CodegenAggregateFunction(init bool) (string, error)
	CodegenMergeFunction() (string, error)
	Defactorize(data interface{}) error
	RequiresInitialization() bool
}

type QueryStepList []QueryStep

//------------------------------------------------------------------------------
//
// Methods
//
//------------------------------------------------------------------------------

//--------------------------------------
// Serialization
//--------------------------------------

// Encodes a query step list into an untyped object.
func (l QueryStepList) Serialize() []interface{} {
	steps := make([]interface{}, 0)
	for _, step := range l {
		steps = append(steps, step.Serialize())
	}
	return steps
}

// Decodes a query step list from an untyped slice.
func DeserializeQueryStepList(obj interface{}, q *Query) (QueryStepList, error) {
	l := make(QueryStepList, 0)
	if steps, ok := obj.([]interface{}); ok {
		for _, _s := range steps {
			if s, ok := _s.(map[string]interface{}); ok {
				var step QueryStep
				switch s["type"] {
				case QueryStepTypeCondition:
					step = NewQueryCondition(q)
				case QueryStepTypeSelection:
					step = NewQuerySelection(q)
				default:
					return nil, fmt.Errorf("Invalid query step type: %v", s["type"])
				}
				err := step.Deserialize(s)
				if err != nil {
					return nil, err
				}
				l = append(l, step)
			} else {
				return nil, fmt.Errorf("Invalid step: %v", obj)
			}
		}
	} else if obj != nil {
		return nil, fmt.Errorf("Invalid steps: %v", obj)
	}
	return l, nil
}

//--------------------------------------
// Code Generation
//--------------------------------------

// Generates aggregate code for all steps.
func (l QueryStepList) CodegenAggregateFunctions(init bool) (string, error) {
	buffer := new(bytes.Buffer)
	for _, step := range l {
		code, err := step.CodegenAggregateFunction(init)
		if err != nil {
			return "", err
		}
		fmt.Fprintln(buffer, code)
	}
	return buffer.String(), nil
}

// Generates merge code for all steps.
func (l QueryStepList) CodegenMergeFunctions() (string, error) {
	buffer := new(bytes.Buffer)
	for _, step := range l {
		code, err := step.CodegenMergeFunction()
		if err != nil {
			return "", err
		}
		fmt.Fprintln(buffer, code)
	}
	return buffer.String(), nil
}

// Generates merge invocations.
func (l QueryStepList) CodegenMergeInvoke() string {
	buffer := new(bytes.Buffer)
	for _, step := range l {
		// Generate this step's invocation if available.
		if step.MergeFunctionName() != "" {
			fmt.Fprintf(buffer, "  %s(results, data)\n", step.MergeFunctionName())
		}

		// Recursively generate child step invocations.
		code := step.GetSteps().CodegenMergeInvoke()
		if code != "" {
			fmt.Fprintf(buffer, code)
		}
	}
	return buffer.String()
}

//--------------------------------------
// Factorization
//--------------------------------------

// Defactorizes results generated from the aggregate function.
func (l QueryStepList) Defactorize(data interface{}) error {
	for _, step := range l {
		err := step.Defactorize(data)
		if err != nil {
			return err
		}
	}
	return nil
}

//--------------------------------------
// Initialization
//--------------------------------------

// Checks if this step requires a data structure to be initialized before
// performing aggregation. This function returns true if any steps require
// initialization.
func (l QueryStepList) RequiresInitialization() bool {
	for _, step := range l {
		if step.RequiresInitialization() {
			return true
		}
	}
	return false
}
