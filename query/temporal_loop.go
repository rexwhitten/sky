package query

import (
	"bytes"
	"errors"
	"fmt"
	"github.com/skydb/sky/core"
)

// A loop statement that iterates over time.
type TemporalLoop struct {
	queryElementImpl
	ref        *VarRef
	step       int
	duration   int
	statements Statements
}

// Creates a new temporal loop.
func NewTemporalLoop() *TemporalLoop {
	return &TemporalLoop{}
}

// Retrieves the function name used during codegen.
func (l *TemporalLoop) FunctionName(init bool) string {
	if init {
		return fmt.Sprintf("i%d", l.ElementId())
	}
	return fmt.Sprintf("a%d", l.ElementId())
}

// Retrieves the merge function name used during codegen.
func (l *TemporalLoop) MergeFunctionName() string {
	return ""
}

// Returns a reference to the variable used for iteration.
func (l *TemporalLoop) Ref() *VarRef {
	return l.ref
}

// Sets the expression.
func (l *TemporalLoop) SetRef(ref *VarRef) {
	if l.ref != nil {
		l.ref.SetParent(nil)
	}
	l.ref = ref
	if l.ref != nil {
		l.ref.SetParent(l)
	}
}

// Returns the statements executed for each loop iteration.
func (l *TemporalLoop) Statements() Statements {
	return l.statements
}

// Sets the loop's statements.
func (l *TemporalLoop) SetStatements(statements Statements) {
	for _, s := range l.statements {
		s.SetParent(nil)
	}
	l.statements = statements
	for _, s := range l.statements {
		s.SetParent(l)
	}
}

//--------------------------------------
// Serialization
//--------------------------------------

// Encodes a temporal loop into an untyped map.
func (l *TemporalLoop) Serialize() map[string]interface{} {
	return map[string]interface{}{
		"type":       TypeTemporalLoop,
		"ref":        l.ref.value,
		"step":       l.step,
		"duration":   l.duration,
		"statements": l.statements.Serialize(),
	}
}

// Decodes a temporal loop from an untyped map.
func (l *TemporalLoop) Deserialize(obj map[string]interface{}) error {
	var err error

	if obj == nil {
		return errors.New("TemporalLoop: Unable to deserialize nil.")
	}
	if obj["type"] != TypeTemporalLoop {
		return fmt.Errorf("TemporalLoop: Invalid statement type: %v", obj["type"])
	}

	// Deserialize "expression".
	if ref, ok := obj["ref"].(string); ok && len(ref) > 0 {
		l.SetRef(&VarRef{value: ref})
	} else {
		return fmt.Errorf("Invalid 'ref': %v", obj["ref"])
	}

	// Deserialize "step".
	if obj["step"] == nil {
		l.step = 0
	} else if step, ok := obj["step"].(float64); ok {
		l.step = int(step)
	} else {
		return fmt.Errorf("Invalid 'step': %v", obj["step"])
	}

	// Deserialize "duration".
	if obj["duration"] == nil {
		l.duration = 0
	} else if duration, ok := obj["duration"].(float64); ok {
		l.duration = int(duration)
	} else {
		return fmt.Errorf("Invalid 'duration': %v", obj["duration"])
	}

	// Parse statements as string or as map.
	var statements Statements
	if strval, ok := obj["statements"].(string); ok && len(strval) > 0 {
		if statements, err = NewStatementsParser().ParseString(strval); err != nil {
			return err
		}
	} else {
		if statements, err = DeserializeStatements(obj["statements"]); err != nil {
			return err
		}
	}
	l.SetStatements(statements)

	return nil
}

//--------------------------------------
// Code Generation
//--------------------------------------

// Generates Lua code for the loop.
func (l *TemporalLoop) CodegenAggregateFunction(init bool) (string, error) {
	buffer := new(bytes.Buffer)

	// Validate.
	if l.step == 0 && l.duration == 0 {
		return "", fmt.Errorf("TemporalLoop: Step or duration must be defined")
	} else if l.duration != 0 && l.step > l.duration {
		return "", fmt.Errorf("TemporalLoop: Step cannot be greater than duration")
	} else if l.ref == nil {
		return "", fmt.Errorf("TemporalLoop: Variable name required")
	}

	// Generate child statement functions.
	str, err := l.statements.CodegenAggregateFunctions(init)
	if err != nil {
		return "", err
	}
	buffer.WriteString(str)

	// Generate variable name.
	ref, err := l.ref.CodegenRaw()
	if err != nil {
		return "", err
	}

	// Generate main function.
	fmt.Fprintf(buffer, "%s\n", lineStartRegex.ReplaceAllString(l.String(), "-- "))
	fmt.Fprintf(buffer, "function %s(cursor, data)\n", l.FunctionName(init))
	fmt.Fprintf(buffer, "  %s = 0\n", ref)
	fmt.Fprintf(buffer, "  start_timestamp = cursor.event:timestamp()\n")
	if l.duration > 0 {
		fmt.Fprintf(buffer, "  end_timestamp = start_timestamp + %d\n", l.duration)
	} else {
		fmt.Fprintf(buffer, "  end_timestamp = 4294967295\n")
	}
	fmt.Fprintf(buffer, "  cursor.max_timestamp = start_timestamp\n")
	fmt.Fprintf(buffer, "  while cursor.max_timestamp < end_timestamp do\n")
	fmt.Fprintf(buffer, "    %s = %s + 1\n", ref, ref)
	fmt.Fprintf(buffer, "    cursor.max_timestamp = cursor.max_timestamp + %d\n", l.step)
	if l.duration == 0 {
		fmt.Fprintf(buffer, "  if cursor:eof() then break end\n")
	}

	// Execute each statement.
	for _, statement := range l.statements {
		fmt.Fprintf(buffer, "    %s(cursor, data)\n", statement.FunctionName(init))
	}

	fmt.Fprintf(buffer, "  end\n")

	// Cleanup.
	fmt.Fprintf(buffer, "  cursor.max_timestamp = 0\n")
	fmt.Fprintf(buffer, "  %s = 0\n", ref)

	// End function definition.
	fmt.Fprintln(buffer, "end")

	return buffer.String(), nil
}

// Generates Lua code for the query.
func (l *TemporalLoop) CodegenMergeFunction(fields map[string]interface{}) (string, error) {
	return l.statements.CodegenMergeFunctions(fields)
}

// Converts factorized fields back to their original strings.
func (l *TemporalLoop) Defactorize(data interface{}) error {
	return l.statements.Defactorize(data)
}

func (l *TemporalLoop) Finalize(data interface{}) error {
	return l.statements.Finalize(data)
}

func (l *TemporalLoop) RequiresInitialization() bool {
	return l.statements.RequiresInitialization()
}

//--------------------------------------
// Utility
//--------------------------------------

// Returns a list of variable references within this condition.
func (l *TemporalLoop) VarRefs() []*VarRef {
	refs := []*VarRef{}
	if l.ref != nil {
		refs = append(refs, l.ref)
	}
	refs = append(refs, l.statements.VarRefs()...)
	return refs
}

// Returns a list of variables declared within this statement. The
// variable used as the counter is automatically declared as an integer.
func (l *TemporalLoop) Variables() []*Variable {
	variables := []*Variable{}
	if l.ref != nil {
		variables = append(variables, NewVariable(l.ref.value, core.IntegerDataType))
	}
	variables = append(variables, l.statements.Variables()...)
	return variables
}

// Converts the loop to a string-based representation.
func (l *TemporalLoop) String() string {
	str := "FOR"
	if l.ref != nil {
		str += " " + l.ref.String()
	}
	if l.step > 0 {
		quantity, units := secondsToTimeSpan(l.step)
		str += " " + fmt.Sprintf("EVERY %d %s", quantity, units)
	}
	if l.duration > 0 {
		quantity, units := secondsToTimeSpan(l.duration)
		str += " " + fmt.Sprintf("WITHIN %d %s", quantity, units)
	}

	str += "\n"
	str += lineStartRegex.ReplaceAllString(l.statements.String(), "  ") + "\n"
	str += "END"

	return str
}
