package memdb

import "reflect"
import "unsafe"

// The byte slice returned by this function will be valid only until the
// original string passed to this function is alive.
func stringToBytes(s string) []byte {
	var hdr reflect.SliceHeader
	shdr := (*reflect.StringHeader)(unsafe.Pointer(&s))
	hdr.Data = shdr.Data
	hdr.Len = shdr.Len
	hdr.Cap = shdr.Len

	return *(*[]byte)(unsafe.Pointer(&hdr))
}
