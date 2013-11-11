package db

import (
	"io/ioutil"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestShardInsertEvent(t *testing.T) {
	withShard(func(s *shard) {
		s.InsertEvent("foo", "bar", testevent("2000-01-01T00:00:00Z", 1, "john"), false)
		e, err := s.getEvent("foo", "bar", musttime("2000-01-01T00:00:00Z"))
		assert.Nil(t, err, "")
		assert.Equal(t, e.Timestamp, musttime("2000-01-01T00:00:00Z"), "")
		assert.Equal(t, e.Data[1], "john", "")
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
