package sparkey

// #include <stdlib.h>
// #include <sparkey/sparkey.h>
import "C"

import (
	"errors"
	"fmt"
	"unsafe"
)

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

// Close the iterator.
func (iter *Iterator) Close() {
	C.sparkey_logiter_close(iter.nativeLI)
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

// Get moves the iterator to the specified key and updates its internal state to Active, Closed, or
// Inactive. This only works if this iterator supports random lookups.
func (iter *Iterator) Get(key string) error {
	if iter.nativeHR == nil {
		return errors.New("this iterator doesn't support random lookups")
	}

	iter.clear()

	ckey := unsafe.Pointer(C.CString(key))
	defer C.free(ckey)

	r, _ := C.sparkey_hash_get(
		*iter.nativeHR,
		(*C.uint8_t)(ckey),
		C.uint64_t(len(key)),
		*iter.nativeLI,
	)

	if err := toErr(r); err != nil {
		return err
	}

	if is, err := iter.State(); is != Active {
		return err
	}

	return iter.readKeyVal(key)
}

// Next iterates to the next entry and updates the internal state of the iterator to Active, Closed,
// or Invalid.
func (iter *Iterator) Next() error {
	iter.clear()

	var r C.sparkey_returncode
	if iter.nativeHR != nil {
		// We have a hashreader, so use it for hashnext()
		r, _ = C.sparkey_logiter_hashnext(*iter.nativeLI, *iter.nativeHR)
	} else {
		// No hashreader available, just use next()
		r, _ = C.sparkey_logiter_next(*iter.nativeLI, *iter.nativeLR)
	}

	if err := toErr(r); err != nil {
		return err
	}
	if is, err := iter.State(); is != Active {
		return err
	}

	return iter.readKeyVal("")
}

func (iter *Iterator) clear() {
	iter.Key = ""
	iter.Value = ""
	iter.Type = 0
}

func (iter *Iterator) readKeyVal(k string) error {
	var key, val cbytes

	if k == "" {
		r, _ := C.sparkey_logiter_keychunk(
			*iter.nativeLI,
			*iter.nativeLR,
			C.uint64_t(^uint64(0)),
			&key.buffer,
			&key.length,
		)
		if err := toErr(r); err != nil {
			return err
		}

		iter.Key = key.String()
	} else {
		// key is known from lookup, just use that
		iter.Key = k
	}

	r, _ := C.sparkey_logiter_valuechunk(
		*iter.nativeLI,
		*iter.nativeLR,
		C.uint64_t(^uint64(0)),
		&val.buffer,
		&val.length,
	)
	if err := toErr(r); err != nil {
		return err
	}

	iter.Value = val.String()

	return nil
}
