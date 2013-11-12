package db

import (
	"bytes"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/skydb/sky/core"
	"github.com/szferi/gomdb"
	"github.com/ugorji/go/codec"
)

// shard represents a subset of the database stored in a single LMDB environment.
type shard struct {
	sync.RWMutex
	path string
	env  *mdb.Env
}

// newShard creates a new shard.
func newShard(path string) *shard {
	return &shard{path: path}
}

// Open allocates a new LMDB environment.
func (s *shard) Open(maxDBs uint, maxReaders uint, options uint) error {
	s.Lock()
	defer s.Unlock()
	s.close()

	if err := os.MkdirAll(s.path, 0700); err != nil {
		return err
	}

	var err error
	s.env, err = mdb.NewEnv()
	if err != nil {
		return fmt.Errorf("lmdb env error: %s", err)
	}

	// LMDB environment settings.
	if err := s.env.SetMaxDBs(mdb.DBI(maxDBs)); err != nil {
		s.close()
		return fmt.Errorf("lmdb maxdbs error: %s", err)
	} else if err := s.env.SetMaxReaders(maxReaders); err != nil {
		s.close()
		return fmt.Errorf("lmdb maxreaders error: %s", err)
	} else if err := s.env.SetMapSize(2 << 40); err != nil {
		s.close()
		return fmt.Errorf("lmdb map size error: %s", err)
	}

	// Open the LMDB environment.
	if err := s.env.Open(s.path, options, 0664); err != nil {
		s.close()
		return fmt.Errorf("lmdb env open error: %s", err)
	}

	return nil
}

// Close releases all shard resources.
func (s *shard) Close() {
	s.Lock()
	defer s.Unlock()
	s.close()
}

func (s *shard) close() {
	if s.env != nil {
		s.env.Close()
		s.env = nil
	}
}

// Cursor retrieves a cursor for iterating over the shard.
func (s *shard) Cursor(tablespace string) (*mdb.Cursor, error) {
	s.RLock()
	defer s.RUnlock()

	txn, dbi, err := s.txn(tablespace, true)
	if err != nil {
		return nil, fmt.Errorf("shard cursor error: %s", err)
	}

	return s.cursor(txn, dbi)
}

// cursor retrieves a cursor for iterating over the shard.
func (s *shard) cursor(txn *mdb.Txn, dbi mdb.DBI) (*mdb.Cursor, error) {
	c, err := txn.CursorOpen(dbi)
	if err != nil {
		return nil, fmt.Errorf("lmdb cursor open error: %s", err)
	}
	return c, nil
}

// InsertEvent adds a single event to the shard.
func (s *shard) InsertEvent(tablespace string, id string, event *core.Event) error {
	s.Lock()
	defer s.Unlock()

	txn, dbi, err := s.txn(tablespace, false)
	if err != nil {
		return fmt.Errorf("lmdb txn begin error: %s", err)
	}

	c, err := s.cursor(txn, dbi)
	if err != nil {
		return fmt.Errorf("lmdb cursor error: %s", err)
	}

	if err := s.insertEvent(txn, dbi, c, id, core.ShiftTimeBytes(event.Timestamp), event.Data); err != nil {
		c.Close()
		txn.Abort()
		return err
	}
	c.Close()

	// Commit the transaction.
	if err := txn.Commit(); err != nil {
		return fmt.Errorf("lmdb txn commit error: %s", err)
	}

	return nil
}

func (s *shard) insertEvent(txn *mdb.Txn, dbi mdb.DBI, c *mdb.Cursor, id string, timestamp []byte, data map[int64]interface{}) error {
	// Get event at timestamp and merge if existing.
	if old, err := s.getEvent(c, id, timestamp); err != nil {
		return err
	} else if old != nil {
		for k, v := range data {
			old[k] = v
		}
		data = old
		if err := c.Del(0); err != nil {
			return fmt.Errorf("lmdb cursor del error: %s", err)
		}
	}

	// Encode timestamp.
	var b bytes.Buffer
	if _, err := b.Write(timestamp); err != nil {
		return err
	}

	// Encode data.
	var handle codec.MsgpackHandle
	handle.RawToString = true
	if err := codec.NewEncoder(&b, &handle).Encode(data); err != nil {
		return err
	}

	// Insert event.
	if err := txn.Put(dbi, []byte(id), b.Bytes(), 0); err != nil {
		return fmt.Errorf("lmdb txn put error: %s", err)
	}

	return nil
}

// InsertEvents adds a multiple events for an object to the shard.
func (s *shard) InsertEvents(tablespace string, id string, events []*core.Event) error {
	s.Lock()
	defer s.Unlock()

	txn, dbi, err := s.txn(tablespace, false)
	if err != nil {
		return fmt.Errorf("lmdb txn begin error: %s", err)
	}

	c, err := s.cursor(txn, dbi)
	if err != nil {
		return fmt.Errorf("lmdb cursor error: %s", err)
	}

	for _, event := range events {
		if err := s.insertEvent(txn, dbi, c, id, core.ShiftTimeBytes(event.Timestamp), event.Data); err != nil {
			c.Close()
			txn.Abort()
			return err
		}
	}
	c.Close()

	// Commit the transaction.
	if err := txn.Commit(); err != nil {
		return fmt.Errorf("lmdb txn commit error: %s", err)
	}

	return nil
}

// Retrieves an event for a given object at a single point in time.
func (s *shard) GetEvent(tablespace string, id string, timestamp time.Time) (*core.Event, error) {
	s.Lock()
	defer s.Unlock()

	txn, dbi, err := s.txn(tablespace, true)
	if err != nil {
		return nil, fmt.Errorf("lmdb txn begin error: %s", err)
	}

	c, err := s.cursor(txn, dbi)
	if err != nil {
		return nil, fmt.Errorf("lmdb cursor error: %s", err)
	}

	data, err := s.getEvent(c, id, core.ShiftTimeBytes(timestamp))
	if err != nil {
		c.Close()
		txn.Abort()
		return nil, err
	}
	c.Close()

	if data == nil {
		return nil, nil
	}

	if err := txn.Commit(); err != nil {
		return nil, fmt.Errorf("lmdb txn commit error: %s", err)
	}

	return &core.Event{Timestamp: timestamp, Data: data}, nil
}

func (s *shard) getEvent(c *mdb.Cursor, id string, timestamp []byte) (map[int64]interface{}, error) {
	// Position cursor at possible event.
	_, _, err := mdbGet2(c, []byte(id), timestamp, mdb.GET_RANGE)
	if err == mdb.NotFound {
		return nil, nil
	} else if err != nil {
		return nil, fmt.Errorf("lmdb cursor get error: %s", err)
	}

	// Retrieve current cursor value.
	_, val, err := c.Get(nil, mdb.GET_CURRENT)
	if err == mdb.NotFound {
		return nil, nil
	} else if err != nil {
		return nil, fmt.Errorf("lmdb cursor get current error: %s", err)
	}

	// Check if timestamp is equal.
	if !bytes.Equal(timestamp, val[0:8]) {
		return nil, nil
	}

	// Decode data.
	var data = make(map[int64]interface{})
	var handle codec.MsgpackHandle
	handle.RawToString = true
	if err := codec.NewDecoder(bytes.NewBuffer(val[8:]), &handle).Decode(&data); err != nil {
		return nil, err
	}
	for k, v := range data {
		data[k] = normalize(v)
	}

	return data, nil
}

// Retrieves a list of events for a given object in a table.
func (s *shard) GetEvents(tablespace string, id string) ([]*core.Event, error) {
	s.Lock()
	defer s.Unlock()

	var events = make([]*core.Event, 0)

	txn, dbi, err := s.txn(tablespace, true)
	if err != nil {
		return nil, fmt.Errorf("lmdb txn begin error: %s", err)
	}

	c, err := s.cursor(txn, dbi)
	if err != nil {
		return nil, fmt.Errorf("lmdb cursor error: %s", err)
	}

	// Initialize cursor.
	if _, _, err := c.Get([]byte(id), mdb.FIRST); err == mdb.NotFound {
		return events, nil
	} else if err != nil {
		return nil, fmt.Errorf("lmdb cursor set first error: %s", err)
	}

	// Initialize cursor position.
	if _, _, err := c.Get([]byte(id), mdb.FIRST_DUP); err == mdb.NotFound {
		return events, nil
	} else if err != nil {
		return nil, fmt.Errorf("lmdb cursor set first dup error: %s", err)
	}

	for {
		_, val, err := c.Get([]byte(id), mdb.GET_CURRENT)
		if err != nil {
			c.Close()
			return nil, fmt.Errorf("lmdb cursor current error: %s", err)
		}

		// Create event.
		event := &core.Event{
			Timestamp: core.UnshiftTimeBytes(val[0:8]),
			Data: make(map[int64]interface{}),
		}
		
		// Decode data.
		var handle codec.MsgpackHandle
		handle.RawToString = true
		if err := codec.NewDecoder(bytes.NewBuffer(val[8:]), &handle).Decode(&event.Data); err != nil {
			c.Close()
			return nil, err
		}
		for k, v := range event.Data {
			event.Data[k] = normalize(v)
		}

		events = append(events, event)

		// Move cursor forward.
		if _, _, err := c.Get([]byte(id), mdb.NEXT_DUP); err == mdb.NotFound {
			break
		} else if err != nil {
			c.Close()
			return nil, fmt.Errorf("lmdb cursor next dup error: %s", err)
		}
	}
	c.Close()

	if err := txn.Commit(); err != nil {
		return nil, fmt.Errorf("lmdb txn commit error: %s", err)
	}

	return events, nil
}

// DeleteEvent removes a single event from the shard.
func (s *shard) DeleteEvent(tablespace string, id string, timestamp time.Time) error {
	s.Lock()
	defer s.Unlock()

	// TODO

	return nil
}

func (s *shard) deleteEvent(txn *mdb.Txn, dbi mdb.DBI, id string, timestamp []byte) error {
	return nil
}

// Deletes all events for a given object in a table.
func (s *shard) DeleteObject(tablespace, id string) error {
	s.Lock()
	defer s.Unlock()

	// Begin a transaction.
	txn, dbi, err := s.txn(tablespace, false)
	if err != nil {
		return fmt.Errorf("shard delete txn error: %s", err)
	}

	// Delete the key.
	if err = txn.Del(dbi, []byte(id), nil); err != nil && err != mdb.NotFound {
		txn.Abort()
		return fmt.Errorf("shard delete error: %s", err)
	}

	// Commit the transaction.
	if err = txn.Commit(); err != nil {
		txn.Abort()
		return fmt.Errorf("shard delete commit error: %s", err)
	}

	return nil
}

// Drop removes a table from the shard.
func (s *shard) Drop(tablespace string) error {
	s.Lock()
	defer s.Unlock()
	return s.drop(tablespace)
}

func (s *shard) drop(tablespace string) error {
	txn, dbi, err := s.txn(tablespace, false)
	if err != nil {
		return fmt.Errorf("drop txn error: %s", err)
	}

	// Drop the table.
	if err = txn.Drop(dbi, 1); err != nil {
		txn.Abort()
		return fmt.Errorf("drop error: %s", err)
	}

	// Commit the transaction.
	if err = txn.Commit(); err != nil {
		txn.Abort()
		return fmt.Errorf("drop commit error: %s", err)
	}

	return nil
}

func (s *shard) txn(tablespace string, readOnly bool) (*mdb.Txn, mdb.DBI, error) {
	var flags uint = 0
	if readOnly {
		flags = flags | mdb.RDONLY
	}

	// Setup cursor to iterate over table.
	txn, err := s.env.BeginTxn(nil, flags)
	if err != nil {
		return nil, 0, fmt.Errorf("Unable to start LMDB transaction: %s", err)
	}
	var dbi mdb.DBI
	if readOnly {
		if dbi, err = txn.DBIOpen(&tablespace, mdb.DUPSORT); err != nil && err != mdb.NotFound {
			return nil, 0, fmt.Errorf("Unable to open read-only LMDB DBI: %s", err)
		}
	} else {
		if dbi, err = txn.DBIOpen(&tablespace, mdb.CREATE|mdb.DUPSORT); err != nil {
			return nil, 0, fmt.Errorf("Unable to open writable LMDB DBI: %s", err)
		}
	}

	return txn, dbi, nil
}
