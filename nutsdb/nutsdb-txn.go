package nutsdb

import (
	"context"
	"fmt"

	"github.com/nutsdb/nutsdb"
	"github.com/sourcenetwork/corekv"
)

type nutsDbTxn struct {
	tx *nutsdb.Tx
	db *Datastore
}

// The tx is created by datastore, it will call db.Begin(writable) to create a new tx
func (t *nutsDbTxn) Set(ctx context.Context, key []byte, value []byte) error {
	// create a local bucket to store uncommitted data
	// map transaction to local bucket

	if err := t.tx.Put(corekvBucket, key, value, 0); err != nil {
		if errRollback := t.rollback(); errRollback != nil {
			return errRollback
		}
	}

	return nil
}

func (txn *nutsDbTxn) Get(ctx context.Context, key []byte) ([]byte, error) {
	return nil, nil
}

func (txn *nutsDbTxn) Has(ctx context.Context, key []byte) (bool, error) {
	return false, nil
}

func (txn *nutsDbTxn) Iterator(ctx context.Context, iterOpts corekv.IterOptions) (corekv.Iterator, error) {
	return nil, nil
}

func (txn *nutsDbTxn) Delete(ctx context.Context, key []byte) error {
	return nil
}

func (txn *nutsDbTxn) Commit() error {
	return txn.Commit()
}

func (txn *nutsDbTxn) Discard() {
	// txn.t.Discard()
}

func (txn *nutsDbTxn) rollback() error {
	if err := txn.tx.Rollback(); err != nil {
		return fmt.Errorf("rollback err: %v", err)
	}
	return nil
}
