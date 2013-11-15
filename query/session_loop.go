package query

import (
	"bytes"
	"errors"
	"fmt"
)

// A loop statement that iterates over individual sessions.
type SessionLoop struct {
	queryElementImpl
	statements   Statements
	IdleDuration int
}

// Creates a new session loop.
func NewSessionLoop() *SessionLoop {
	return &SessionLoop{}
}

// Retrieves the function name used during codegen.
func (l *SessionLoop) FunctionName(init bool) string {
	if init {
		return fmt.Sprintf("i%d", l.ElementId())
	}
	return fmt.Sprintf("a%d", l.ElementId())
}

// Retrieves the merge function name used during codegen.
func (l *SessionLoop) MergeFunctionName() string {
	return ""
}

// Returns the statements executed for each loop iteration.
func (l *SessionLoop) Statements() Statements {
	return l.statements
}

// Sets the loop's statements.
func (l *SessionLoop) SetStatements(statements Statements) {
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

// Encodes a session loop into an untyped map.
func (l *SessionLoop) Serialize() map[string]interface{} {
	return map[string]interface{}{
		"type":       TypeSessionLoop,
		"statements": l.statements.Serialize(),
	}
}

// Decodes a session loop from an untyped map.
func (l *SessionLoop) Deserialize(obj map[string]interface{}) error {
	var err error

	if obj == nil {
		return errors.New("SessionLoop: Unable to deserialize nil.")
	} else if obj["type"] != TypeSessionLoop {
		return fmt.Errorf("SessionLoop: Invalid statement type: %v", obj["type"])
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
func (l *SessionLoop) CodegenAggregateFunction(init bool) (string, error) {
	buffer := new(bytes.Buffer)

	// Generate child statement functions.
	str, err := l.statements.CodegenAggregateFunctions(init)
	if err != nil {
		return "", err
	}
	buffer.WriteString(str)

	fmt.Fprintf(buffer, "%s\n", lineStartRegex.ReplaceAllString(l.String(), "-- "))
	fmt.Fprintf(buffer, "function %s(cursor, data)\n", l.FunctionName(init))
	fmt.Fprintf(buffer, "  cursor:set_session_idle(%d)\n", l.IdleDuration)
	fmt.Fprintln(buffer, "  while true do")
	fmt.Fprintln(buffer, "    repeat")
	for _, statement := range l.statements {
		fmt.Fprintf(buffer, "      %s(cursor, data)\n", statement.FunctionName(init))
	}
	fmt.Fprintln(buffer, "    until not cursor:next()")
	fmt.Fprintln(buffer, "  if not cursor:next_session() then break end")
	fmt.Fprintln(buffer, "  cursor:next()")
	fmt.Fprintln(buffer, "  end")
	fmt.Fprintln(buffer, "  cursor:set_session_idle(0)")
	fmt.Fprintln(buffer, "end")

	return buffer.String(), nil
}

// Generates Lua code for the query.
func (l *SessionLoop) CodegenMergeFunction(fields map[string]interface{}) (string, error) {
	return l.statements.CodegenMergeFunctions(fields)
}

// Converts factorized fields back to their original strings.
func (l *SessionLoop) Defactorize(data interface{}) error {
	return l.statements.Defactorize(data)
}

func (l *SessionLoop) Finalize(data interface{}) error {
	return l.statements.Finalize(data)
}

func (l *SessionLoop) RequiresInitialization() bool {
	return l.statements.RequiresInitialization()
}

//--------------------------------------
// Utility
//--------------------------------------

// Returns a list of variable references within this condition.
func (l *SessionLoop) VarRefs() []*VarRef {
	return l.statements.VarRefs()
}

// Returns a list of variables declared within this statement.
func (l *SessionLoop) Variables() []*Variable {
	return l.statements.Variables()
}

// Converts the loop to a string-based representation.
func (l *SessionLoop) String() string {
	quantity, units := secondsToTimeSpan(l.IdleDuration)
	str := fmt.Sprintf("FOR EACH SESSION DELIMITED BY %d %s\n", quantity, units)
	str += lineStartRegex.ReplaceAllString(l.statements.String(), "  ") + "\n"
	str += "END"
	return str
}
