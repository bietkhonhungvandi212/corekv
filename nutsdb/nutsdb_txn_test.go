package nutsdb

import (
	"context"
	"testing"

	"github.com/nutsdb/nutsdb"
	"github.com/sourcenetwork/corekv"
	"github.com/stretchr/testify/require"
)

var (

	// committed keys
	committedKeys = [][]byte{
		[]byte("key1"),
		[]byte("key2"),
		[]byte("key5"),
		[]byte("key6"),
		[]byte("key9"),
	}

	// uncommitted keys
	uncommittedKeys = [][]byte{
		[]byte("key3"),
		[]byte("key4"),
		[]byte("key7"),
		[]byte("key8"),
	}
)

func TestNutsDBTxn_IterateCommittedKeys(t *testing.T) {
	runTestNutsDB(t, func(t *testing.T, ds *Datastore) {
		for _, key := range committedKeys {
			require.NoError(t, ds.Set(context.Background(), key, []byte("value")))
		}

		for i := range committedKeys {
			require.NoError(t, ds.db.View(func(tx *nutsdb.Tx) error {
				val, err := tx.Get(corekvBucket, committedKeys[i])
				require.NoError(t, err)
				require.Equal(t, val, []byte("value"))
				return nil
			}))
		}

		iter, err := ds.Iterator(context.Background(), corekv.IterOptions{Reverse: false})
		require.NoError(t, err)
		defer iter.Close()

		for i := range committedKeys {
			iterKey := iter.Key()
			require.Equal(t, committedKeys[i], iterKey)

			hasNext, err := iter.Next()
			require.NoError(t, err)

			if !hasNext {
				break
			}
		}
	})
}
