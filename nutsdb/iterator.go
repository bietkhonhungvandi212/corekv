package nutsdb

import (
	"github.com/sourcenetwork/corekv"
	"github.com/tidwall/btree"
)

type iterator struct {
	d *Datastore

	values *btree.BTreeG[dsItem]
	it     btree.IterG[dsItem]

	// If true, the iterator will iterate in reverse order, from the largest
	// key to the smallest.
	reverse bool

	// reset is a mutatuble property that indicates whether the iterator should be
	// returned to the beginning on the next [Next] call.
	reset bool
	valid bool
}

// var _ corekv.Iterator = (*iterator)(nil)

func newIterator(d *Datastore, values *btree.BTreeG[dsItem], reverse bool) *iterator {
	return &iterator{
		d:       d,
		values:  values,
		it:      values.Iter(),
		reverse: reverse,
		reset:   true,
	}
}

func (iter *iterator) Next() (bool, error) {
	iter.d.closeLk.RLock()
	defer iter.d.closeLk.RUnlock()
	if iter.d.closed {
		return false, corekv.ErrDBClosed
	}

	if !iter.valid {
		return false, nil
	}

	if iter.reverse {
		iter.valid = iter.it.Prev()
	} else {
		iter.valid = iter.it.Next()
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
