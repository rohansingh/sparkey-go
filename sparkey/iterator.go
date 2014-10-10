package sparkey

// #include <sparkey/sparkey.h>
import "C"

import "fmt"

// Iterator is a stateful iterator that is associated with a specific Reader. It holds a reference
// to the underlying C iterator as well.
type Iterator struct {
	nativeLI **C.struct_sparkey_logiter
	nativeLR **C.struct_sparkey_logreader
	nativeHR **C.struct_sparkey_hashreader

	Key   string
	Value string
	Type  EntryType
}

// IterState is the state of an iterator.
type IterState int

const (
	_ IterState = iota
	New
	Active
	Closed
	Invalid
)

func (is *IterState) load(c C.sparkey_iter_state) error {
	switch c {
	case C.SPARKEY_ITER_NEW:
		*is = New
	case C.SPARKEY_ITER_ACTIVE:
		*is = Active
	case C.SPARKEY_ITER_CLOSED:
		*is = Closed
	case C.SPARKEY_ITER_INVALID:
		*is = Invalid
	default:
		return fmt.Errorf("unknown sparkey_iter_state: %v", c)
	}

	return nil
}

func newIterator(lr **C.struct_sparkey_logreader, hr **C.struct_sparkey_hashreader) (*Iterator, error) {
	var n *C.struct_sparkey_logiter
	r, _ := C.sparkey_logiter_create(&n, *lr)

	if err := toErr(r); err != nil {
		return nil, err
	}

	iter := &Iterator{
		nativeLI: &n,
		nativeLR: lr,
		nativeHR: hr,
	}

	return iter, nil
}

// State returns the IterState of the Iterator.
func (iter *Iterator) State() (IterState, error) {
	var is IterState

	cis, err := C.sparkey_logiter_state(*iter.nativeLI)
	if err != nil {
		return is, err
	}

	return is, is.load(cis)
}

// Next iterates to the next entry and updates the internal state of the iterator to Active, Closed,
// or Invalid.
func (iter *Iterator) Next() (IterState, error) {
	iter.Key = ""
	iter.Value = ""
	iter.Type = 0

	if iter.nativeHR != nil {
		// We have a hashreader, so use it for hashnext()
		C.sparkey_logiter_hashnext(*iter.nativeLI, *iter.nativeHR)
	} else {
		// No hashreader available, just use next()
		C.sparkey_logiter_next(*iter.nativeLI, *iter.nativeLR)
	}

	is, err := iter.State()
	if is != Active {
		return is, err
	}

	var key, val cbytes

	r, _ := C.sparkey_logiter_keychunk(
		*iter.nativeLI,
		*iter.nativeLR,
		C.uint64_t(^uint64(0)),
		&key.buffer,
		&key.length,
	)
	r2, _ := C.sparkey_logiter_valuechunk(
		*iter.nativeLI,
		*iter.nativeLR,
		C.uint64_t(^uint64(0)),
		&val.buffer,
		&val.length,
	)

	if err := toErr(r); err != nil {
		return is, err
	}
	if err := toErr(r2); err != nil {
		return is, err
	}

	iter.Key = key.String()
	iter.Value = val.String()

	return is, nil
}
