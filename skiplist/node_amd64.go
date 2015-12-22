package skiplist

import (
	"sync/atomic"
	"unsafe"
)

// Node structure overlaps with an array of NodeRef struct
//
//  <Node struct>
// +--------------+-----------------+----------------+
// | itm - 8bytes | GClink - 8bytes | level = 2 bytes|  <[]NodeRef struct>
// +--------------+-----------------+----------------+-----+--------------+--------------+--------------+
//                                  | flag - 8bytes        | ptr - 8 bytes| flag - 8bytes| ptr - 8 bytes|
//                                  +----------------------+--------------+--------------+--------------+

var nodeHdrSize = unsafe.Sizeof(struct {
	itm    unsafe.Pointer
	GClink *Node
}{})

var nodeRefSize = unsafe.Sizeof(NodeRef{})

const deletedFlag = 0xff

type Node struct {
	itm    unsafe.Pointer
	GClink *Node
	level  uint16
}

func (n Node) Level() int {
	return int(n.level)
}

func (n Node) Size() int {
	return int(nodeHdrSize + uintptr(n.level+1)*nodeRefSize)
}

func (n *Node) Item() unsafe.Pointer {
	return n.itm
}

func (n *Node) SetLink(l *Node) {
	n.GClink = l
}

func (n *Node) GetLink() *Node {
	return n.GClink
}

type NodeRef struct {
	flag uint64
	ptr  *Node
}

func newNode(itm unsafe.Pointer, level int) *Node {
	n := (*Node)(allocNode(level))
	n.level = uint16(level)
	n.itm = itm
	return n
}

func (n *Node) setNext(level int, ptr *Node, deleted bool) {
	ref := (*NodeRef)(unsafe.Pointer(uintptr(unsafe.Pointer(n)) + nodeHdrSize + nodeRefSize*uintptr(level)))
	ref.ptr = ptr
}

func (n *Node) getNext(level int) (*Node, bool) {
	nodeRefAddr := uintptr(unsafe.Pointer(n)) + nodeHdrSize + nodeRefSize*uintptr(level)
	wordAddr := (*uint64)(unsafe.Pointer(nodeRefAddr + uintptr(7)))

	v := atomic.LoadUint64(wordAddr)
	deleted := v&deletedFlag == deletedFlag
	ptr := (*Node)(unsafe.Pointer(uintptr(v >> 8)))
	return ptr, deleted
}

// The node struct holds a slice of NodeRef. We assume that the
// most-significant-byte of the golang pointer is always unused. In NodeRef
// struct, deleted flag and *Node are packed one after the other.
// If we shift the node address 1 byte to the left. The shifted 8 byte word will have
// a byte from the deleted flag and 7 bytes from the address (8th byte of the address
// is always 0x00). CAS operation can be performed at this location to set
// least-significant to 0xff (denotes deleted). Same applies for loading delete
// flag and the address atomically.
func (n *Node) dcasNext(level int, prevPtr, newPtr *Node, prevIsdeleted, newIsdeleted bool) bool {
	nodeRefAddr := uintptr(unsafe.Pointer(n)) + nodeHdrSize + nodeRefSize*uintptr(level)
	wordAddr := (*uint64)(unsafe.Pointer(nodeRefAddr + uintptr(7)))
	prevVal := uint64(uintptr(unsafe.Pointer(prevPtr)) << 8)
	newVal := uint64(uintptr(unsafe.Pointer(newPtr)) << 8)

	if newIsdeleted {
		newVal |= deletedFlag
	}

	return atomic.CompareAndSwapUint64(wordAddr, prevVal, newVal)
}
