package factors

import (
	"errors"
	"fmt"
	"github.com/skydb/sky/core"
	"github.com/szferi/gomdb"
	"os"
	"reflect"
	"strconv"
	"sync"
)

//------------------------------------------------------------------------------
//
// Typedefs
//
//------------------------------------------------------------------------------

// A Factors database object manages the factorization and defactorization of values.
type DB struct {
	env   *mdb.Env
	path  string
	mutex sync.Mutex
}

//------------------------------------------------------------------------------
//
// Errors
//
//------------------------------------------------------------------------------

//--------------------------------------
// Factor Not Found
//--------------------------------------

func NewFactorNotFound(text string) error {
	return &FactorNotFound{text}
}

type FactorNotFound struct {
	s string
}

func (e *FactorNotFound) Error() string {
	return e.s
}

//------------------------------------------------------------------------------
//
// Constructors
//
//------------------------------------------------------------------------------

// NewDB returns a new database object.
func NewDB(path string) *DB {
	return &DB{path: path}
}

//------------------------------------------------------------------------------
//
// Accessors
//
//------------------------------------------------------------------------------

// The path to the database on disk.
func (db *DB) Path() string {
	return db.path
}

//------------------------------------------------------------------------------
//
// Methods
//
//------------------------------------------------------------------------------

//--------------------------------------
// State
//--------------------------------------

// Opens the factors databse.
func (db *DB) Open() error {
	var err error
	if db.IsOpen() {
		return errors.New("skyd: Factors database is already open.")
	}

	// Create the factors directory.
	if err = os.MkdirAll(db.path, 0700); err != nil {
		return err
	}

	// Create the database environment.
	if db.env, err = mdb.NewEnv(); err != nil {
		return fmt.Errorf(fmt.Sprintf("skyd: Unable to create factors environment: %v", err))
	}
	// Setup max dbs.
	if err = db.env.SetMaxDBs(4096); err != nil {
		db.Close()
		return fmt.Errorf("skyd: Unable to set factors max dbs: %v", err)
	}
	// Setup map size.
	if err := db.env.SetMapSize(10 << 30); err != nil {
		return fmt.Errorf("skyd: Unable to set factors map size: %v", err)
	}
	// Open the database.
	if err = db.env.Open(db.path, 0, 0664); err != nil {
		db.Close()
		return fmt.Errorf("skyd: Cannot open factors database (%s): %s", db.path, err)
	}

	return nil
}

// Closes the factors database.
func (db *DB) Close() {
	if db.env != nil {
		db.env.Close()
		db.env = nil
	}
}

// Returns whether the factors database is open.
func (db *DB) IsOpen() bool {
	return db.env != nil
}

//--------------------------------------
// DB
//--------------------------------------

// Retrieves the value from the database for a given key.
func (db *DB) get(namespace string, key string) (string, bool, error) {
	txn, err := db.env.BeginTxn(nil, 0)
	if err != nil {
		return "", false, fmt.Errorf("skyd: Unable to start factors get txn: %s", err)
	}
	dbi, err := txn.DBIOpen(&namespace, mdb.CREATE)
	if err != nil {
		return "", false, fmt.Errorf("skyd: Unable to open factors DBI [get]: %s", err)
	}

	// Retrieve byte array.
	data, err := txn.Get(dbi, []byte(key))
	if err != nil && err != mdb.NotFound {
		err = fmt.Errorf("skyd: Unable to get factor: %s", err)
		fmt.Fprintln(os.Stderr, err.Error())
		txn.Abort()
		return "", false, err
	}
	txn.Abort()

	return string(data), (data != nil), nil
}

// Sets the value for a given key in the database.
func (db *DB) put(namespace string, key string, value string) error {
	txn, err := db.env.BeginTxn(nil, 0)
	if err != nil {
		return fmt.Errorf("skyd: Unable to start factors put txn: %s", err)
	}
	dbi, err := txn.DBIOpen(&namespace, mdb.CREATE)
	if err != nil {
		return fmt.Errorf("skyd: Unable to open factors DBI [put]: %s", err)
	}

	// Set value for key.
	if err = txn.Put(dbi, []byte(key), []byte(value), mdb.NODUPDATA); err != nil {
		err = fmt.Errorf("skyd: Unable to put factor: %s", err)
		fmt.Fprintln(os.Stderr, err.Error())
		txn.Abort()
		return err
	}
	if err = txn.Commit(); err != nil {
		err = fmt.Errorf("skyd: Unable to commit factor: %s", err)
		fmt.Fprintln(os.Stderr, err.Error())
		txn.Abort()
		return err
	}

	return nil
}

//--------------------------------------
// Keys
//--------------------------------------

// The key for a given namespace/id/value.
func (db *DB) key(id string, value string) string {
	return fmt.Sprintf("%x:%s>%s", len(id), id, value)
}

// The reverse key for a given namespace/id/value.
func (db *DB) revkey(id string, value uint64) string {
	return fmt.Sprintf("%x:%s<%d", len(id), id, value)
}

// The sequence key for a given namespace/id.
func (db *DB) seqkey(id string) string {
	return fmt.Sprintf("%x:%s!", len(id), id)
}

//--------------------------------------
// Factorization
//--------------------------------------

// Converts the defactorized value for a given id in a given namespace to its internal representation.
func (db *DB) Factorize(namespace string, id string, value string, createIfMissing bool) (uint64, error) {
	// Blank is always zero.
	if value == "" {
		return 0, nil
	}

	// Otherwise find it in the database.
	data, exists, err := db.get(namespace, db.key(id, value))
	if err != nil {
		return 0, err
	}
	// If key does exist then parse and return it.
	if exists {
		return strconv.ParseUint(string(data), 10, 64)
	}

	// Create a new factor if requested.
	if createIfMissing {
		return db.add(namespace, id, value)
	}

	err = NewFactorNotFound(fmt.Sprintf("skyd: Factor not found: %v", db.key(id, value)))
	return 0, err
}

// Adds a new factor to the database if it doesn't exist.
func (db *DB) add(namespace string, id string, value string) (uint64, error) {
	// Lock while adding a new value.
	db.mutex.Lock()
	defer db.mutex.Unlock()

	// Retry factorize within the context of the lock.
	sequence, err := db.Factorize(namespace, id, value, false)
	if err == nil {
		return sequence, nil
	} else if _, ok := err.(*FactorNotFound); !ok {
		return 0, err
	}

	// Retrieve next id in sequence.
	sequence, err = db.inc(namespace, id)
	if err != nil {
		return 0, err
	}

	// Save lookup and reverse lookup.
	if err = db.put(namespace, db.key(id, value), strconv.FormatUint(sequence, 10)); err != nil {
		return 0, err
	}
	if err = db.put(namespace, db.revkey(id, sequence), value); err != nil {
		return 0, err
	}

	return sequence, nil
}

// Converts the factorized value for a given id in a given namespace to its internal representation.
func (db *DB) Defactorize(namespace string, id string, value uint64) (string, error) {
	// Blank is always zero.
	if value == 0 {
		return "", nil
	}

	// Find it in the database.
	data, exists, err := db.get(namespace, db.revkey(id, value))
	if err != nil {
		return "", err
	}
	if !exists {
		return "", fmt.Errorf("skyd: Factor value does not exist: %v", db.revkey(id, value))
	}
	return string(data), nil
}

// Retrieves the next available sequence number within a namespace for an id.
func (db *DB) inc(namespace string, id string) (uint64, error) {
	data, exists, err := db.get(namespace, db.seqkey(id))
	if err != nil {
		return 0, err
	}

	// Initialize key if it doesn't exist. Otherwise increment it.
	if !exists {
		if err := db.put(namespace, db.seqkey(id), "1"); err != nil {
			return 0, err
		}
		return 1, nil
	}

	// Parse existing sequence.
	sequence, err := strconv.ParseUint(string(data), 10, 64)
	if err != nil {
		return 0, fmt.Errorf("skyd: Unable to parse factor sequence: %v", data)
	}

	// Increment and save the new value.
	sequence += 1
	if err = db.put(namespace, db.seqkey(id), strconv.FormatUint(sequence, 10)); err != nil {
		return 0, err
	}
	return sequence, nil
}

//--------------------------------------
// Event Factorization
//--------------------------------------

// Factorizes the values in an event.
func (db *DB) FactorizeEvent(event *core.Event, namespace string, propertyFile *core.PropertyFile, createIfMissing bool) error {
	if event == nil {
		return nil
	}

	for k, v := range event.Data {
		property := propertyFile.GetProperty(k)
		if property.DataType == core.FactorDataType {
			if stringValue, ok := v.(string); ok {
				sequence, err := db.Factorize(namespace, property.Name, stringValue, createIfMissing)
				if err != nil {
					return err
				}
				event.Data[k] = sequence
			}
		}
	}

	return nil
}

// Defactorizes the values in an event.
func (db *DB) DefactorizeEvent(event *core.Event, namespace string, propertyFile *core.PropertyFile) error {
	if event == nil {
		return nil
	}

	for k, v := range event.Data {
		property := propertyFile.GetProperty(k)
		if property.DataType == core.FactorDataType {
			if sequence, ok := castUint64(v); ok {
				stringValue, err := db.Defactorize(namespace, property.Name, sequence)
				if err != nil {
					return err
				}
				event.Data[k] = stringValue
			}
		}
	}

	return nil
}

//--------------------------------------
// Utility
//--------------------------------------

// Casts to a uint64 if possible.
func castUint64(value interface{}) (uint64, bool) {
	v := reflect.ValueOf(value)
	switch v.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return uint64(v.Int()), true
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return uint64(v.Uint()), true
	case reflect.Float32, reflect.Float64:
		return uint64(v.Float()), true
	}
	return 0, false
}
