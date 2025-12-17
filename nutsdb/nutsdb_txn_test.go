package nutsdb

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNutsDBTxn_Iterate(t *testing.T) {
	// committed keys
	committedKeys := [][]byte{
		[]byte("key1"),
		[]byte("key2"),
		[]byte("key5"),
		[]byte("key6"),
		[]byte("key9"),
	}

	// uncommitted keys
	// key3 := []byte("key3")
	// key4 := []byte("key4")
	// key7 := []byte("key7")
	// key8 := []byte("key8")

	runTestNutsDB(t, func(t *testing.T, ds *Datastore) {
		for _, key := range committedKeys {
			require.NoError(t, ds.Set(context.Background(), key, []byte("value")))
		}

		// iter, err := ds.Iterator(context.Background(), corekv.IterOptions{ Reverse: false })

		// txn, err := ds.newTxn(true)
	})
}
