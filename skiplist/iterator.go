package skiplist

import "sync/atomic"
import "unsafe"

type Iterator struct {
	cmp        CompareFn
	s          *Skiplist
	prev, curr *Node
	valid      bool
	buf        *ActionBuffer
	deleted    bool

	bs *BarrierSession
}

func (s *Skiplist) NewIterator(cmp CompareFn,
	buf *ActionBuffer) *Iterator {

	return &Iterator{
		cmp: cmp,
		s:   s,
		buf: buf,
		bs:  s.barrier.Acquire(),
	}
}

func (it *Iterator) SeekFirst() {
	it.prev = it.s.head
	it.curr, _ = it.s.head.getNext(0)
	it.valid = true
}

func (it *Iterator) SeekWithCmp(itm unsafe.Pointer, cmp CompareFn, eqCmp CompareFn) bool {
	var found bool
	if found = it.s.findPath(itm, cmp, it.buf, &it.s.Stats) != nil; found {
		it.prev = it.buf.preds[0]
		it.curr = it.buf.succs[0]
	} else {
		if found = eqCmp != nil && compare(eqCmp, itm, it.buf.preds[0].Item()) == 0; found {
			it.prev = nil
			it.curr = it.buf.preds[0]
		}
	}
	return found
}

func (it *Iterator) Seek(itm unsafe.Pointer) bool {
	it.valid = true
	found := it.s.findPath(itm, it.cmp, it.buf, &it.s.Stats) != nil
	it.prev = it.buf.preds[0]
	it.curr = it.buf.succs[0]
	return found
}

// If the specified item is not found, start with the predecessor node
// This is used for implementing disk block based storage
func (it *Iterator) SeekPrev(itm unsafe.Pointer) {
	if !it.Seek(itm) {
		it.curr = it.prev
		it.prev = nil
	}
}

func (it *Iterator) Valid() bool {
	if it.valid && it.curr == it.s.tail {
		it.valid = false
	}

	return it.valid
}

func (it *Iterator) Get() unsafe.Pointer {
	return it.curr.Item()
}

func (it *Iterator) GetNode() *Node {
	return it.curr
}

func (it *Iterator) Delete() {
	it.s.softDelete(it.curr, &it.s.Stats)
	// It will observe that current item is deleted
	// Run delete helper and move to the next possible item
	it.Next()
	it.deleted = true
}

func (it *Iterator) Next() {
	if it.deleted {
		it.deleted = false
		return
	}

retry:
	it.valid = true
	next, deleted := it.curr.getNext(0)
	if deleted {
		// Current node is deleted. Unlink current node from the level
		// and make next node as current node.
		// If it fails, refresh the path buffer and obtain new current node.
		if it.prev != nil && it.s.helpDelete(0, it.prev, it.curr, next, &it.s.Stats) {
			it.curr = next
		} else {
			atomic.AddUint64(&it.s.Stats.readConflicts, 1)
			found := it.s.findPath(it.curr.Item(), it.cmp, it.buf, &it.s.Stats) != nil
			last := it.curr
			it.prev = it.buf.preds[0]
			it.curr = it.buf.succs[0]
			if found && last == it.curr {
				goto retry
			}
		}
	} else {
		it.prev = it.curr
		it.curr = next
	}
}

func (it *Iterator) Close() {
	it.s.barrier.Release(it.bs)
}
