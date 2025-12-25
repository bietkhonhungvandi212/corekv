package nutsdb

import (
	"context"
	"fmt"
	"sync"

	"github.com/nutsdb/nutsdb"
	"github.com/sourcenetwork/corekv"
)

const (
	corekvBucket = "corekv"
)

type Datastore struct {
	db     *nutsdb.DB
	closed bool

	closeLk sync.RWMutex
}

type dsItem struct {
	key       []byte
	val       []byte
	isDeleted bool
	isGet     bool
}

func NewDatastore(path string, opts ...nutsdb.Option) (*Datastore, error) {
	db, err := nutsdb.Open(
		nutsdb.DefaultOptions,
		append(opts, nutsdb.WithDir(path))...,
	)

	if err != nil {
		return nil, err
	}

	// Use a global bucket to store the corekv data
	if err := db.Update(func(tx *nutsdb.Tx) error {
		return tx.NewBucket(nutsdb.DataStructureBTree, corekvBucket)
	}); err != nil {
		return nil, err
	}

	return &Datastore{db: db}, nil
}

func (d *Datastore) Set(ctx context.Context, key []byte, value []byte) error {
	txn, ok := corekv.TryGetCtxTxnG[*nutsDbTxn](ctx)
	if ok {
		return txn.Set(ctx, key, value)
	}

	txn, err := d.newTxn(true)

	// This is implementation of NutsDB when fail to create a new txn
	defer func() {
		if r := recover(); r != nil {
			fmt.Printf("panic when executing tx, err is %+v", r)
		}
	}()

	defer txn.Discard() // NOTE: havent been implemented yet

	if err != nil {
		return err
	}

	if err = txn.Set(ctx, key, value); err != nil {
		return err
	}

	return txn.Commit()
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

func (ds *Datastore) newTxn(writable bool) (*nutsDbTxn, error) {
	tx, err := ds.db.Begin(writable)
	if err != nil {
		return nil, err
	}

	return &nutsDbTxn{tx: tx, ds: ds}, nil
}

func (d *Datastore) Iterator(ctx context.Context, opts corekv.IterOptions) (corekv.Iterator, error) {
	tx, ok := corekv.TryGetCtxTxnG[*nutsDbTxn](ctx)
	if ok {
		return tx.Iterator(ctx, opts)
	}
	d.closeLk.RLock()
	defer d.closeLk.RUnlock()
	if d.closed {
		return nil, corekv.ErrDBClosed
	}

	// if opts.Prefix != nil {
	// 	return newPrefixIter(d, d.values, opts.Prefix, opts.Reverse, d.getVersion()), nil
	// }
	// return newRangeIter(d, d.values, opts.Start, opts.End, opts.Reverse, d.getVersion()), nil
	txn, err := d.newTxn(false)
	if err != nil {
		return nil, err
	}

	iter, err := txn.Iterator(ctx, opts)
	if err != nil {
		return nil, err
	}

	// if the iterator is call not in a transaction,
	// we need to commit the transaction when the iterator is closed
	// to release the lock in nutsdb transaction
	if nutsIter, ok := iter.(*iterator); ok {
		nutsIter.withCloser(func() error {
			txn.Commit() //TODO: can replace with Discard() ?
			return nil
		})
	}

	return iter, nil
}
