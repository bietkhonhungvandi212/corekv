package nutsdb

import (
	"context"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

const nutsDBTestDirPath = "/tmp/nutsdb-test"

func removeDir(dir string) {
	if err := os.RemoveAll(dir); err != nil {
		panic(err)
	}
}

func newNutsDBDatastore() *Datastore {
	defer removeDir(nutsDBTestDirPath)

	if db, err := NewDatastore(nutsDBTestDirPath); err != nil {
		panic(err)
	} else {
		return db
	}
}

func runTestNutsDB(t *testing.T, test func(t *testing.T, db *Datastore)) {
	ds := newNutsDBDatastore()
	test(t, ds)

	t.Cleanup(func() {
		if !ds.db.IsClose() {
			require.NoError(t, ds.db.Close())
		}
	})
}

func TestNutsDB_Set(t *testing.T) {
	runTestNutsDB(t, func(t *testing.T, ds *Datastore) {
		err := ds.Set(context.Background(), []byte("testKey"), []byte("testValue"))
		require.NoError(t, err)
	})
}
