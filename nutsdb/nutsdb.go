package nutsdb

import (
	"context"

	"github.com/nutsdb/nutsdb"
)

const (
	corekvBucket = "corekv"
)

type Datastore struct {
	db     *nutsdb.DB
	closed bool
}

func NewDatastore(path string, opts ...nutsdb.Option) (*Datastore, error) {
	db, err := nutsdb.Open(
		nutsdb.DefaultOptions,
		append(opts, nutsdb.WithDir(path))...,
	)

	if err != nil {
		return nil, err
	}

	if err := db.Update(func(tx *nutsdb.Tx) error {
		return tx.NewBucket(nutsdb.DataStructureBTree, corekvBucket)
	}); err != nil {
		return nil, err
	}

	return &Datastore{db: db}, nil
}

// TODO: Set the key
func (d *Datastore) Set(ctx context.Context, key []byte, value []byte) error {
	//TODO
	return nil
}

// func (d *Datastore) Set(ctx context.Context, key []byte, value []byte) error {
// 	txn, ok := corekv.TryGetCtxTxnG[*nutsDbTxn](ctx)
// 	if ok {
// 		return txn.Set(ctx, key, value)
// 	}

// 	txn, err := d.newTxn(true)
// 	if err != nil {
// 		return err
// 	}

// 	//TODO: handle the discard with rollback
// 	defer txn.Discard()
// 	txn.Set(ctx, key, value)

// 	return txn.Commit()
// }

func (d *Datastore) newTxn(writable bool) (*nutsDbTxn, error) {
	tx, err := d.db.Begin(writable)
	if err != nil {
		return nil, err
	}
	return &nutsDbTxn{tx: tx}, nil
}
