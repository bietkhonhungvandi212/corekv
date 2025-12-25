package nutsdb

import (
	"context"
	"fmt"

	"github.com/nutsdb/nutsdb"
	"github.com/sourcenetwork/corekv"
	"github.com/tidwall/btree"
)

type nutsDbTxn struct {
	tx *nutsdb.Tx
	ds *Datastore

	pendingItems *btree.BTreeG[dsItem]
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
	// txn.ds.closeLk.RLock()
	// defer txn.ds.closeLk.RUnlock()
	if txn.ds.closed {
		return nil, corekv.ErrDBClosed
	}

	return NewIterator(txn.ds, txn.tx, txn.pendingItems, iterOpts.Reverse), nil
}

func (txn *nutsDbTxn) Delete(ctx context.Context, key []byte) error {
	return nil
}

func (txn *nutsDbTxn) Commit() error {
	return txn.tx.Commit()
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
