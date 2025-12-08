package nutsdb

import (
	"context"

	"github.com/nutsdb/nutsdb"
	"github.com/sourcenetwork/corekv"
)

type nutsDbTxn struct {
	tx *nutsdb.Tx
}

func (t *nutsDbTxn) Set(ctx context.Context, key []byte, value []byte) error {
	// return t.tx.Set(key, value)
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
	return nil
}

func (txn *nutsDbTxn) Discard() {
	// txn.t.Discard()
}
