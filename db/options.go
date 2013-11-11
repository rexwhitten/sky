package db

import (
	"github.com/szferi/gomdb"
)

// options creates an LMDB flagset.
func options(noSync bool) uint {
	flagset := uint(0)
	flagset |= mdb.NOTLS
	if noSync {
		flagset |= mdb.NOSYNC
	}
	return flagset
}
