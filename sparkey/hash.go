package sparkey

// #include <stdlib.h>
// #include <sparkey/sparkey.h>
import "C"

import "unsafe"

// HashSize selects the hash size to use when writing the hash file.
type HashSize int

const (
	Auto                    HashSize = 0
	MurmurHash3_32                   = 4
	MurmurHash3_128_Lower64          = 8
)

// WriteHash creates a hash file for the specified log file, which must already exist. It's safe
// and efficient to run this multiple times. If the hash file already exists, it will be used to
// speed up the creation of the new file by reusing the existing entrie.
//
// Note that the hash file is never overwritten. Instead the old file is unlinked and the new one
// is created. Thus, it's safe to rewrite the hash table while other processes are reading from it.
func WriteHash(hashFilename string, logFilename string, hashSize HashSize) error {
	chf, clf := C.CString(hashFilename), C.CString(logFilename)
	defer C.free(unsafe.Pointer(chf))
	defer C.free(unsafe.Pointer(clf))

	r, _ := C.sparkey_hash_write(
		chf,
		clf,
		C.int(hashSize),
	)
	return toErr(r)
}
