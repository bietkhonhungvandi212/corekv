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

func (d *Datastore) Set(ctx context.Context, key []byte, value []byte) error {
	return nil
}
