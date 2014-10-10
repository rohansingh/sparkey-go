package sparkey

// #cgo LDFLAGS: -lsparkey
// #include <sparkey/sparkey.h>
import "C"

import "fmt"

// SparkeyError represents an error returned by the Sparkey C library.
type SparkeyError struct {
	errno int
}

var returnCodes = map[int]string{
	0:  "SUCCESS",
	-1: "INTERNAL_ERROR",

	-100: "FILE_NOT_FOUND",
	-101: "PERMISSION_DENIED",
	-102: "TOO_MANY_OPEN_FILES",
	-103: "FILE_TOO_LARGE",
	-104: "FILE_ALREADY_EXISTS",
	-105: "FILE_BUSY",
	-106: "FILE_IS_DIRECTORY",
	-107: "FILE_SIZE_EXCEEDED",
	-108: "FILE_CLOSED",
	-109: "OUT_OF_DISK",
	-110: "UNEXPECTED_EOF",
	-111: "MMAP_FAILED",

	-200: "WRONG_LOG_MAGIC_NUMBER",
	-201: "WRONG_LOG_MAJOR_VERSION",
	-202: "UNSUPPORTED_LOG_MINOR_VERSION",
	-203: "LOG_TOO_SMALL",
	-204: "LOG_CLOSED",
	-205: "LOG_ITERATOR_INACTIVE",
	-206: "LOG_ITERATOR_MISMATCH",
	-207: "LOG_ITERATOR_CLOSED",
	-208: "LOG_HEADER_CORRUPT",
	-209: "INVALID_COMPRESSION_BLOCK_SIZE",
	-210: "INVALID_COMPRESSION_TYPE",

	-300: "WRONG_HASH_MAGIC_NUMBER",
	-301: "WRONG_HASH_MAJOR_VERSION",
	-302: "UNSUPPORTED_HASH_MINOR_VERSION",
	-303: "HASH_TOO_SMALL",
	-304: "HASH_CLOSED",
	-305: "FILE_IDENTIFIER_MISMATCH",
	-306: "HASH_HEADER_CORRUPT",
	-307: "HASH_SIZE_INVALID",
}

func (s SparkeyError) Error() string {
	return fmt.Sprintf("sparkey: %s", returnCodes[s.errno])
}

func toErr(r C.sparkey_returncode) error {
	if r == 0 {
		return nil
	} else {
		return SparkeyError{errno: int(r)}
	}
}

// CompressionType is the type of compression (if any) to use for the log file.
type CompressionType int

const (
	None CompressionType = iota
	Snappy
)

func (ct CompressionType) c() C.sparkey_compression_type {
	switch ct {
	case Snappy:
		return C.SPARKEY_COMPRESSION_SNAPPY
	default:
		return C.SPARKEY_COMPRESSION_NONE
	}
}

func (ct *CompressionType) load(c C.sparkey_compression_type) error {
	switch c {
	case C.SPARKEY_COMPRESSION_NONE:
		*ct = None
	case C.SPARKEY_COMPRESSION_SNAPPY:
		*ct = Snappy
	default:
		return fmt.Errorf("unknown sparkey_compression_type: %v", c)
	}

	return nil
}

// EntryType is the type of an entry in the Sparkey log file (i.e., PUT, DELETE).
type EntryType int

const (
	_ EntryType = iota
	Put
	Delete
)

func (et *EntryType) load(c C.sparkey_entry_type) error {
	switch c {
	case C.SPARKEY_ENTRY_PUT:
		*et = Put
	case C.SPARKEY_ENTRY_DELETE:
		*et = Delete
	default:
		return fmt.Errorf("unknown sparkey_entry_type: %v", c)
	}

	return nil
}
