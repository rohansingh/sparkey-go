package sparkey

// #include <stdlib.h>
// #include <sparkey/sparkey.h>
import "C"

import "unsafe"

// Use Reader to read and iterate a log file. This struct holds a reference to the underlying
// C object.
type Reader struct {
	nativeLR **C.struct_sparkey_logreader
	nativeHR **C.struct_sparkey_hashreader

	SupportsRandomLookups bool
	LogFilename           string
	HashFilename          string

	Compression          CompressionType
	CompressionBlockSize int
	MaxKeyLen            uint64
	MaxValueLen          uint64
}

// NewReader returns a Reader for reading an existing Sparkey log file, and optionally a hash
// file. If a hash filename is provided, the hash file will be used to support random lookups.
// Otherwise, only sequential iteration will be supported.
func NewReader(logFilename string, hashFilename string) (*Reader, error) {
	cfn := C.CString(logFilename)
	defer C.free(unsafe.Pointer(cfn))

	var n *C.struct_sparkey_logreader
	r, _ := C.sparkey_logreader_open(&n, cfn)

	if err := toErr(r); err != nil {
		return nil, err
	}

	re := &Reader{
		nativeLR:     &n,
		LogFilename:  logFilename,
		HashFilename: hashFilename,
	}

	if err := re.openHashReader(); err != nil {
		defer re.Close()
		return nil, err
	}

	if err := re.loadHeader(); err != nil {
		defer re.Close()
		return nil, err
	}

	return re, nil
}

func (re *Reader) openHashReader() error {
	if re.HashFilename == "" {
		return nil
	}

	clf, chf := C.CString(re.LogFilename), C.CString(re.HashFilename)
	defer C.free(unsafe.Pointer(clf))
	defer C.free(unsafe.Pointer(chf))

	var n *C.struct_sparkey_hashreader
	r, _ := C.sparkey_hash_open(&n, chf, clf)

	if err := toErr(r); err != nil {
		return err
	}

	re.nativeHR = &n
	re.SupportsRandomLookups = true

	return nil
}

func (re *Reader) loadHeader() error {
	var ct CompressionType
	sct, err := C.sparkey_logreader_get_compression_type(*re.nativeLR)
	if err != nil {
		return err
	}

	ct.load(sct)

	cbs, err := C.sparkey_logreader_get_compression_blocksize(*re.nativeLR)
	if err != nil {
		return err
	}

	cmk, err := C.sparkey_logreader_maxkeylen(*re.nativeLR)
	if err != nil {
		return err
	}

	cmv, err := C.sparkey_logreader_maxvaluelen(*re.nativeLR)
	if err != nil {
		return err
	}

	re.Compression = ct
	re.CompressionBlockSize = int(cbs)
	re.MaxKeyLen = uint64(cmk)
	re.MaxValueLen = uint64(cmv)

	return nil
}

// Close the Reader. This will cause further operations on any open LogIterator to fail.
func (re *Reader) Close() {
	C.sparkey_logreader_close(re.nativeLR)

	if re.SupportsRandomLookups {
		C.sparkey_hash_close(re.nativeHR)
	}
}

// Iter initializes a LogIter and associates it with a Reader. If this Reader supports random
// lookups, the LogIter will only stop at live entries (i.e., entries that are of type Put, and that
// have not been overwritten or deleted). Otherwise, the LogIter will stop at all entries, including
// Put, Delete, and overwritten or deleted entries.
//
// The Reader must not be closed. The returned LogIter is not thread-safe.
func (re *Reader) Iter() (*Iterator, error) {
	return newIterator(re.nativeLR, re.nativeHR)
}
