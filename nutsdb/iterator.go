package nutsdb

import (
	"github.com/tidwall/btree"
)

// Refer to the memory/iter.go file for more information on the iterator implementation.
type iterator struct {
	d *Datastore

	version uint64
	values  *btree.BTreeG[dsItem]
	it      btree.IterG[dsItem]

	// If true, the iterator will iterate in reverse order, from the largest
	// key to the smallest.
	reverse bool

	// reset is a mutatuble property that indicates whether the iterator should be
	// returned to the beginning on the next [Next] call.
	reset bool

	// These properties are used by a work around for a bug in the btree implementation:
	// https://github.com/tidwall/btree/issues/46 - these properties and the work around
	// should be removed when the btree bug is fixed.
	//
	// Currently it is believed that this is only required for the `Reverse` option (tidwall bug
	// appears to be directional).
	//
	// `TestBTreePrevBug` also documents this issue.
	lastItem  dsItem
	firstItem dsItem
}

func newIterator(d *Datastore, values *btree.BTreeG[dsItem], reverse bool) *iterator {
	return &iterator{
		d:       d,
		values:  values,
		it:      values.Iter(),
		reverse: reverse,
		reset:   true,
	}
}
