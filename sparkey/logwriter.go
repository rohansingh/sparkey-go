package sparkey

// #include <stdlib.h>
// #include <sparkey/sparkey.h>
import "C"

import (
	"unsafe"
)

// LogWriter adds entries to a log file. This struct holds a reference to the underlying C object.
type LogWriter struct {
	native **C.struct_sparkey_logwriter

	Filename string
}

// CreateLog creates a new Sparkey log file, possibly overwriting an existing one, and returns
// a LogWriter for writing to it.
func CreateLog(filename string, ct CompressionType, compressionBlockSize int) (*LogWriter, error) {
	cfn := C.CString(filename)
	defer C.free(unsafe.Pointer(cfn))

	var n *C.struct_sparkey_logwriter
	r, _ := C.sparkey_logwriter_create(
		&n,
		cfn,
		ct.c(),
		C.int(compressionBlockSize))

	if err := toErr(r); err != nil {
		return nil, err
	}

	return &LogWriter{native: &n, Filename: filename}, nil
}

// AppendLog returns a LogWriter for appending to an existing Sparkey log file.
func AppendLog(filename string) (*LogWriter, error) {
	cfn := C.CString(filename)
	defer C.free(unsafe.Pointer(cfn))

	var n *C.struct_sparkey_logwriter
	r, _ := C.sparkey_logwriter_append(&n, cfn)

	if err := toErr(r); err != nil {
		return nil, err
	}

	return &LogWriter{native: &n, Filename: filename}, nil
}

// Flush and close the log file.
func (lw LogWriter) Close() error {
	r, _ := C.sparkey_logwriter_close(lw.native)
	return toErr(r)
}

// Flush the open compression block (if applicable) to the buffer, flush the file buffer to disk,
// and rewrite the header on disk, enabling readers to read from the log.
func (lw LogWriter) Flush() error {
	r, _ := C.sparkey_logwriter_flush(*lw.native)
	return toErr(r)
}

// Put appends a key/value pair to the log file.
func (lw LogWriter) Put(key string, val string) error {
	ckey, cval := unsafe.Pointer(C.CString(key)), unsafe.Pointer(C.CString(val))
	defer C.free(ckey)
	defer C.free(cval)

	r, _ := C.sparkey_logwriter_put(
		*lw.native,
		C.uint64_t(len(key)),
		(*C.uint8_t)(ckey),
		C.uint64_t(len(val)),
		(*C.uint8_t)(cval),
	)
	return toErr(r)
}

// Delete appends a delete operation for a key to the log file.
func (lw LogWriter) Delete(key string) error {
	ckey := unsafe.Pointer(C.CString(key))
	defer C.free(ckey)

	r, _ := C.sparkey_logwriter_delete(
		*lw.native,
		C.uint64_t(len(key)),
		(*C.uint8_t)(ckey),
	)
	return toErr(r)
}
