package sparkey

// #cgo LDFLAGS: -lsparkey
// #include <stdlib.h>
// #include <sparkey/sparkey.h>
import "C"

import "fmt"

type SparkeyError struct {
	errno int
}

// const (
// 	SUCCESS        SparkeyError = 0
// 	INTERNAL_ERROR SparkeyError = -1
//
// 	FILE_NOT_FOUND      SparkeyError = -100
// 	PERMISSION_DENIED   SparkeyError = -101
// 	TOO_MANY_OPEN_FILES SparkeyError = -102
// 	FILE_TOO_LARGE      SparkeyError = -103
// 	FILE_ALREADY_EXISTS SparkeyError = -104
// 	FILE_BUSY           SparkeyError = -105
// 	FILE_IS_DIRECTORY   SparkeyError = -106
// 	FILE_SIZE_EXCEEDED  SparkeyError = -107
// 	FILE_CLOSED         SparkeyError = -108
// 	OUT_OF_DISK         SparkeyError = -109
// 	UNEXPECTED_EOF      SparkeyError = -110
// 	MMAP_FAILED         SparkeyError = -111
//
// 	WRONG_LOG_MAGIC_NUMBER         SparkeyError = -200
// 	WRONG_LOG_MAJOR_VERSION        SparkeyError = -201
// 	UNSUPPORTED_LOG_MINOR_VERSION  SparkeyError = -202
// 	LOG_TOO_SMALL                  SparkeyError = -203
// 	LOG_CLOSED                     SparkeyError = -204
// 	LOG_ITERATOR_INACTIVE          SparkeyError = -205
// 	LOG_ITERATOR_MISMATCH          SparkeyError = -206
// 	LOG_ITERATOR_CLOSED            SparkeyError = -207
// 	LOG_HEADER_CORRUPT             SparkeyError = -208
// 	INVALID_COMPRESSION_BLOCK_SIZE SparkeyError = -209
// 	INVALID_COMPRESSION_TYPE       SparkeyError = -210
//
// 	WRONG_HASH_MAGIC_NUMBER        SparkeyError = -300
// 	WRONG_HASH_MAJOR_VERSION       SparkeyError = -301
// 	UNSUPPORTED_HASH_MINOR_VERSION SparkeyError = -302
// 	HASH_TOO_SMALL                 SparkeyError = -303
// 	HASH_CLOSED                    SparkeyError = -304
// 	FILE_IDENTIFIER_MISMATCH       SparkeyError = -305
// 	HASH_HEADER_CORRUPT            SparkeyError = -306
// 	HASH_SIZE_INVALID              SparkeyError = -307
// )

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
