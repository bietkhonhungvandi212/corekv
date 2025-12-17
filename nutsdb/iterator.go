package nutsdb

import (
	"bytes"

	"github.com/nutsdb/nutsdb"
	"github.com/sourcenetwork/corekv"
	"github.com/tidwall/btree"
)

type iterator struct {
	d          *Datastore
	nutsdbIter *nutsdb.Iterator

	values *btree.BTreeG[dsItem]
	it     btree.IterG[dsItem]

	// If true, the iterator will iterate in reverse order, from the largest
	// key to the smallest.
	reverse bool

	// reset is a mutatuble property that indicates whether the iterator should be
	// returned to the beginning on the next [Next] call.
	reset bool
	valid bool

	currentPendingKey []byte
	currentNutsdbKey  []byte

	currentKey []byte
}

// var _ corekv.Iterator = (*iterator)(nil)

func NewIterator(d *Datastore, tx *nutsdb.Tx, values *btree.BTreeG[dsItem], reverse bool) *iterator {
	return &iterator{
		d:          d,
		nutsdbIter: nutsdb.NewIterator(tx, corekvBucket, nutsdb.IteratorOptions{Reverse: reverse}),
		values:     values,
		it:         values.Iter(),
		reverse:    reverse,
		reset:      true,
	}
}

// pendingKeys - 1 - 2 - 5 - 6 -9
// nutsdbIter - 3 - 4 - 7 - 8

// hasPendingKey: true -> pendingKey = 1
// hasNutsdbKey: true -> nutsdbKey = 3
// currentKey: 1
func (iter *iterator) Next() (bool, error) {
	iter.d.closeLk.RLock()
	defer iter.d.closeLk.RUnlock()
	if iter.d.closed {
		return false, corekv.ErrDBClosed
	}

	if iter.valid == false {
		return false, nil
	}

	hasNutsdbKey := false
	hasPendingKey := false

	// For first iteration, we need to setup current key for pending and committed nutsdb
	if len(iter.currentPendingKey) == 0 && len(iter.currentNutsdbKey) == 0 {
		if iter.reverse {
			hasPendingKey = iter.it.Prev()
		} else {
			hasPendingKey = iter.it.Next()
		}

		hasNutsdbKey = iter.nutsdbIter.Next()

		if hasPendingKey {
			iter.currentPendingKey = iter.it.Item().key
		}

		if hasNutsdbKey {
			iter.currentNutsdbKey = iter.nutsdbIter.Key()
		}

		iter.valid = hasNutsdbKey || hasPendingKey
	} else {
		// We just call check next
		if len(iter.currentPendingKey) > 0 && bytes.Compare(iter.currentPendingKey, iter.currentNutsdbKey) < 0 {
			if iter.reverse {
				hasPendingKey = iter.it.Prev()
			} else {
				hasPendingKey = iter.it.Next()
			}

			if hasPendingKey {
				iter.currentPendingKey = iter.it.Item().key
			} else {
				iter.currentPendingKey = nil
			}
		} else {
			hasNutsdbKey = iter.nutsdbIter.Next()

			if hasPendingKey {
				iter.currentNutsdbKey = iter.nutsdbIter.Key()
			} else {
				iter.currentNutsdbKey = nil
			}
		}

		iter.valid = hasNutsdbKey || hasPendingKey
	}

	return iter.valid, nil
}

func (iter *iterator) Key() []byte {
	if !iter.valid {
		return nil
	}

	// Note we can cache the current item in the iterator
	return iter.it.Item().key
}

// Value returns the value at the current iterator location.
//
// If the iterator is currently at an invalid location it's behaviour is undefined:
// https://github.com/sourcenetwork/corekv/issues/37
func (iter *iterator) Value() ([]byte, error) {
	if !iter.valid {
		return nil, nil
	}

	return iter.it.Item().val, nil
}

// Seek moves the iterator to the given key, if an exact match is not found, the
// iterator will progress to the next valid value (depending on the `Reverse` option).
//
// Seek will return `true` if it found a valid item, otherwise `false`.
//
// Seek will not seek to values outside of the constraints provided in [IterOptions].
func (iter *iterator) Seek([]byte) (bool, error) {
	if !iter.valid {
		return false, nil
	}

	// Note: implement this
	// return iter.it.Seek(key), nil
	return false, nil
}

// Reset resets the iterator, allowing for re-iteration.
// TODO: implement this
func (iter *iterator) Reset() {
	iter.valid = false
	// iter.it.Reset()
}

// Close releases the iterator.
// TODO: implement this
func (iter *iterator) Close() error {
	iter.it.Release()
	return nil
}
