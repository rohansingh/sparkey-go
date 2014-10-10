package sparkey

// #cgo LDFLAGS: -lsparkey
// #include <sparkey/sparkey.h>
import "C"

import "unsafe"

type cbytes struct {
	buffer *C.uint8_t
	length C.uint64_t
}

func (cb cbytes) String() string {
	ulen := uintptr(cb.length)
	buf := make([]byte, ulen)

	uptr := uintptr(unsafe.Pointer(cb.buffer))

	for i := uintptr(0); i < ulen; i++ {
		ptr := unsafe.Pointer(uptr + i)
		buf[i] = byte(*(*C.uint8_t)(ptr))
	}

	return string(buf)
}
