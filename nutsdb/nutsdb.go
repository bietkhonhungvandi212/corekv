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
	version   uint64
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

func (d *Datastore) newTxn(writable bool) (*nutsDbTxn, error) {
	tx, err := d.db.Begin(writable)
	if err != nil {
		return nil, err
	}

	return &nutsDbTxn{tx: tx, db: d}, nil
}
