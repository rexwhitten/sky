package core

import (
	"encoding/binary"
	"time"
)

// Shifts a Go time into Sky timestamp format.
func ShiftTime(value time.Time) int64 {
	timestamp := value.UnixNano() / 1000
	usec := timestamp % 1000000
	sec := timestamp / 1000000
	return (sec << 20) + usec
}

// ShiftTimeBytes converts a Go time in to a byte slice in Sky timestamp format.
func ShiftTimeBytes(value time.Time) []byte {
	var b [8]byte
	bs := b[:8]
	timestamp := ShiftTime(value)
	binary.BigEndian.PutUint64(bs, uint64(timestamp))
	return bs
}

// Shifts a Sky timestamp format into a Go time.
func UnshiftTime(value int64) time.Time {
	usec := value & 0xFFFFF
	sec := value >> 20
	return time.Unix(sec, usec*1000)
}
