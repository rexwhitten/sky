package db

import (
	"io/ioutil"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestShardInsertEvent(t *testing.T) {
	withShard(func(s *shard) {
		s.InsertEvent("tbl0", "obj0", testevent("2000-01-01T00:00:00Z", 1, "john"))
		e, err := s.GetEvent("tbl0", "obj0", musttime("2000-01-01T00:00:00Z"))
		assert.Nil(t, err, "")
		assert.Equal(t, e.Timestamp, musttime("2000-01-01T00:00:00Z"), "")
		assert.Equal(t, e.Data[1], "john", "")
	})
}

func TestShardMergeEvent(t *testing.T) {
	withShard(func(s *shard) {
		s.InsertEvent("tbl0", "obj0", testevent("2000-01-01T00:00:00Z", 1, "aaa", 2, 100))
		s.InsertEvent("tbl0", "obj0", testevent("2000-01-03T00:00:00Z", 1, "bbb", 2, 200))
		s.InsertEvent("tbl0", "obj0", testevent("2000-01-02T00:00:00Z", 1, "ccc", 2, 300))
		s.InsertEvent("tbl0", "obj0", testevent("2000-01-02T00:00:00Z", 1, "ddd"))
		e, err := s.GetEvent("tbl0", "obj0", musttime("2000-01-02T00:00:00Z"))
		assert.Nil(t, err, "")
		assert.Equal(t, e.Timestamp, musttime("2000-01-02T00:00:00Z"), "")
		assert.Equal(t, e.Data[1], "ddd", "")
		assert.Equal(t, e.Data[2], 300, "")
	})
}

func TestShardGetMissingEventInExistingObject(t *testing.T) {
	withShard(func(s *shard) {
		s.InsertEvent("tbl0", "obj0", testevent("2000-01-01T00:00:00Z", 1, "john"))
		e, err := s.GetEvent("tbl0", "obj0", musttime("1990-01-01T00:00:00Z"))
		assert.Nil(t, e, "")
		assert.Nil(t, err, "")
	})
}

func TestShardGetMissingEventInMissingObject(t *testing.T) {
	withShard(func(s *shard) {
		s.InsertEvent("tbl0", "obj0", testevent("2000-01-01T00:00:00Z", 1, "john"))
		e, err := s.GetEvent("tbl0", "wrong_obj", musttime("2000-01-01T00:00:00Z"))
		assert.Nil(t, e, "")
		assert.Nil(t, err, "")
	})
}

func withShard(f func(*shard)) {
	path, _ := ioutil.TempDir("", "")
	defer os.RemoveAll(path)

	s := newShard(path)
	if err := s.Open(4096, 126, options(false)); err != nil {
		panic(err.Error())
	}
	defer s.Close()

	f(s)
}
