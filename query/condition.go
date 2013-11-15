package query

import (
	"bytes"
	"errors"
	"fmt"
	"strings"
)

const (
	UnitSteps    = "steps"
	UnitSessions = "sessions"
	UnitSeconds  = "seconds"
)

// A condition statement made within a query.
type Condition struct {
	queryElementImpl
	expression       Expression
	WithinRangeStart int
	WithinRangeEnd   int
	WithinUnits      string
	statements       Statements
}

// Creates a new condition.
func NewCondition() *Condition {
	return &Condition{
		WithinRangeStart: 0,
		WithinRangeEnd:   0,
		WithinUnits:      UnitSteps,
	}
}

// Retrieves the function name used during codegen.
func (c *Condition) FunctionName(init bool) string {
	if init {
		return fmt.Sprintf("i%d", c.ElementId())
	}
	return fmt.Sprintf("a%d", c.ElementId())
}

// Retrieves the merge function name used during codegen.
func (c *Condition) MergeFunctionName() string {
	return ""
}

// Returns the expression evaluated for truth by the condition.
func (c *Condition) Expression() Expression {
	return c.expression
}

// Sets the expression.
func (c *Condition) SetExpression(expression Expression) {
	if c.expression != nil {
		c.expression.SetParent(nil)
	}
	c.expression = expression
	if c.expression != nil {
		c.expression.SetParent(c)
	}
}

// Returns the statements executed if the condition expression is true.
func (c *Condition) Statements() Statements {
	return c.statements
}

// Sets the condition's statements.
func (c *Condition) SetStatements(statements Statements) {
	for _, s := range c.statements {
		s.SetParent(nil)
	}
	c.statements = statements
	for _, s := range c.statements {
		s.SetParent(c)
	}
}

//--------------------------------------
// Serialization
//--------------------------------------

// Encodes a query condition into an untyped map.
func (c *Condition) Serialize() map[string]interface{} {
	return map[string]interface{}{
		"type":        TypeCondition,
		"expression":  c.expression.String(),
		"within":      []int{c.WithinRangeStart, c.WithinRangeEnd},
		"withinUnits": c.WithinUnits,
		"statements":  c.statements.Serialize(),
	}
}

// Decodes a query condition from an untyped map.
func (c *Condition) Deserialize(obj map[string]interface{}) error {
	var err error

	if obj == nil {
		return errors.New("Condition: Unable to deserialize nil.")
	}
	if obj["type"] != TypeCondition {
		return fmt.Errorf("Condition: Invalid statement type: %v", obj["type"])
	}

	// Deserialize "expression".
	if expression, ok := obj["expression"].(string); ok {
		parser := NewExpressionParser()
		expr, err := parser.ParseString(expression)
		if err != nil {
			return err
		}
		c.SetExpression(expr)
	} else {
		if obj["expression"] == nil {
			c.SetExpression(&BooleanLiteral{value: true})
		} else {
			return fmt.Errorf("Invalid 'expression': %v", obj["expression"])
		}
	}

	// Deserialize "within" range.
	if withinRange, ok := obj["within"].([]interface{}); ok && len(withinRange) == 2 {
		if withinRangeStart, ok := withinRange[0].(float64); ok {
			c.WithinRangeStart = int(withinRangeStart)
		} else {
			return fmt.Errorf("Condition: Invalid 'within' range start: %v", withinRange[0])
		}
		if withinRangeEnd, ok := withinRange[1].(float64); ok {
			c.WithinRangeEnd = int(withinRangeEnd)
		} else {
			return fmt.Errorf("Condition: Invalid 'within' range end: %v", withinRange[1])
		}
	} else {
		if obj["within"] == nil {
			c.WithinRangeStart = 0
			c.WithinRangeEnd = 0
		} else {
			return fmt.Errorf("Invalid 'within' range: %v", obj["within"])
		}
	}

	// Deserialize "within units".
	if withinUnits, ok := obj["withinUnits"].(string); ok {
		switch withinUnits {
		case UnitSteps, UnitSessions, UnitSeconds:
			c.WithinUnits = withinUnits
		default:
			return fmt.Errorf("Invalid 'within units': %v", withinUnits)
		}
	} else {
		if obj["withinUnits"] == nil {
			c.WithinUnits = UnitSteps
		} else {
			return fmt.Errorf("Invalid 'within units': %v", obj["withinUnits"])
		}
	}

	// DEPRECATED: Statements can be passed in as "steps".
	val := obj["steps"]
	if val == nil {
		val = obj["statements"]
	}

	// Parse statements as string or as map.
	var statements Statements
	if strval, ok := val.(string); ok && len(strval) > 0 {
		if statements, err = NewStatementsParser().ParseString(strval); err != nil {
			return err
		}
	} else {
		if statements, err = DeserializeStatements(val); err != nil {
			return err
		}
	}
	c.SetStatements(statements)

	return nil
}

//--------------------------------------
// Code Generation
//--------------------------------------

// Generates Lua code for the query.
func (c *Condition) CodegenAggregateFunction(init bool) (string, error) {
	buffer := new(bytes.Buffer)

	// Validate.
	if c.WithinRangeStart > c.WithinRangeEnd {
		return "", fmt.Errorf("Condition: Invalid 'within' range: %d..%d", c.WithinRangeStart, c.WithinRangeEnd)
	}

	// Generate child statement functions.
	str, err := c.statements.CodegenAggregateFunctions(init)
	if err != nil {
		return "", err
	}
	buffer.WriteString(str)

	// Generate main function.
	fmt.Fprintf(buffer, "%s\n", lineStartRegex.ReplaceAllString(c.String(), "-- "))
	fmt.Fprintf(buffer, "function %s(cursor, data)\n", c.FunctionName(init))
	if c.WithinRangeStart > 0 {
		fmt.Fprintf(buffer, "  if cursor:eos() or cursor:eof() then return false end\n")
	}
	if c.WithinUnits == UnitSteps {
		fmt.Fprintf(buffer, "  index = 0\n")
	}
	fmt.Fprintf(buffer, "  repeat\n")
	if c.WithinUnits == UnitSteps {
		fmt.Fprintf(buffer, "    if index >= %d and index <= %d then\n", c.WithinRangeStart, c.WithinRangeEnd)
	}

	// Generate conditional expression.
	expressionCode, err := c.expression.Codegen()
	if err != nil {
		return "", err
	}
	fmt.Fprintf(buffer, "      if %s then\n", expressionCode)

	// Call each statement function.
	for _, statement := range c.statements {
		fmt.Fprintf(buffer, "        %s(cursor, data)\n", statement.FunctionName(init))
	}

	fmt.Fprintf(buffer, "        return true\n")
	fmt.Fprintf(buffer, "      end\n")
	fmt.Fprintf(buffer, "    end\n")
	if c.WithinUnits == UnitSteps {
		fmt.Fprintf(buffer, "    if index >= %d then break end\n", c.WithinRangeEnd)
		fmt.Fprintf(buffer, "    index = index + 1\n")
	}
	fmt.Fprintf(buffer, "  until not cursor:next()\n")
	fmt.Fprintf(buffer, "  return false\n")

	// End function definition.
	fmt.Fprintln(buffer, "end")

	return buffer.String(), nil
}

// Generates Lua code for the query.
func (c *Condition) CodegenMergeFunction(fields map[string]interface{}) (string, error) {
	buffer := new(bytes.Buffer)

	// Generate child statement functions.
	str, err := c.statements.CodegenMergeFunctions(fields)
	if err != nil {
		return "", err
	}
	buffer.WriteString(str)

	return buffer.String(), nil
}

// Converts factorized fields back to their original strings.
func (c *Condition) Defactorize(data interface{}) error {
	return c.statements.Defactorize(data)
}

// Finalizes the results into a final state after merge.
func (c *Condition) Finalize(data interface{}) error {
	return c.statements.Finalize(data)
}

//--------------------------------------
// Initialization
//--------------------------------------

// Checks if this condition requires a data structure to be initialized before
// performing aggregation. This function returns true if any nested query
// statements require initialization.
func (c *Condition) RequiresInitialization() bool {
	return c.statements.RequiresInitialization()
}

//--------------------------------------
// Utility
//--------------------------------------

// Returns a list of variable references within this condition.
func (c *Condition) VarRefs() []*VarRef {
	refs := []*VarRef{}
	refs = append(refs, c.expression.VarRefs()...)
	refs = append(refs, c.statements.VarRefs()...)
	return refs
}

// Returns a list of variables declared within this statement.
func (c *Condition) Variables() []*Variable {
	return c.statements.Variables()
}

// Converts the condition to a string-based representation.
func (c *Condition) String() string {
	str := "WHEN"
	if str != "" {
		str += " " + c.expression.String()
	}
	if c.WithinRangeStart != 0 || c.WithinRangeStart != 0 || c.WithinUnits != UnitSteps {
		str += fmt.Sprintf(" WITHIN %d .. %d %s", c.WithinRangeStart, c.WithinRangeEnd, strings.ToUpper(c.WithinUnits))
	}
	str += " THEN\n"
	str += lineStartRegex.ReplaceAllString(c.statements.String(), "  ") + "\n"
	str += "END"
	return str
}
