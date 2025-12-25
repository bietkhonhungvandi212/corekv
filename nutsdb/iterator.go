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
	closer     func() error
}

// var _ corekv.Iterator = (*iterator)(nil)

func NewIterator(d *Datastore, tx *nutsdb.Tx, values *btree.BTreeG[dsItem], reverse bool) *iterator {
	var pendingIter btree.IterG[dsItem]
	var firstPendingKey []byte

	if values != nil {
		pendingIter = values.Iter()
		firstPendingKey = pendingIter.Item().key
	}

	nutsdbIter := nutsdb.NewIterator(tx, corekvBucket, nutsdb.IteratorOptions{Reverse: reverse})
	firstNutsdbKey := nutsdbIter.Key()

	return &iterator{
		d:                 d,
		nutsdbIter:        nutsdbIter,
		values:            values,
		it:                pendingIter,
		reverse:           reverse,
		reset:             true,
		valid:             len(firstPendingKey) > 0 || len(firstNutsdbKey) > 0,
		currentNutsdbKey:  firstNutsdbKey,
		currentPendingKey: firstPendingKey,
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

		if hasNutsdbKey {
			iter.currentNutsdbKey = iter.nutsdbIter.Key()
		} else {
			iter.currentNutsdbKey = nil
		}
	}

	iter.valid = hasNutsdbKey || hasPendingKey

	return iter.valid, nil
}

func (iter *iterator) Key() []byte {
	if !iter.valid {
		return nil
	}

	if len(iter.currentNutsdbKey) == 0 && len(iter.currentPendingKey) == 0 {
		return nil
	}

	// Note we can cache the current item in the iterator
	if len(iter.currentNutsdbKey) > 0 && len(iter.currentPendingKey) == 0 {
		return iter.currentNutsdbKey
	}

	if len(iter.currentPendingKey) > 0 && len(iter.currentNutsdbKey) == 0 {
		return iter.currentPendingKey
	}

	if bytes.Compare(iter.currentNutsdbKey, iter.currentPendingKey) < 0 {
		return iter.currentNutsdbKey
	}

	return iter.currentPendingKey
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

	if iter.closer != nil {
		return iter.closer()
	}
	return nil
}

func (iter *iterator) withCloser(closer func() error) {
	iter.closer = closer
}
