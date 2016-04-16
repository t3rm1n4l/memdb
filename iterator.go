package memdb

import (
	"github.com/t3rm1n4l/memdb/skiplist"
	"unsafe"
)

type Iterator struct {
	count       int
	refreshRate int

	snap *Snapshot
	iter *skiplist.Iterator
	buf  *skiplist.ActionBuffer

	blockBuf []byte

	block dataBlock
	curr  []byte
}

func (it *Iterator) skipUnwanted() {
loop:
	if !it.iter.Valid() {
		return
	}
	itm := (*Item)(it.iter.Get())
	if itm.bornSn > it.snap.sn || (itm.deadSn > 0 && itm.deadSn <= it.snap.sn) {
		it.iter.Next()
		it.count++
		goto loop
	}
}

func (it *Iterator) loadItems() {
	if it.snap.db.HasBlockStore() {
		n := it.GetNode()
		if err := it.snap.db.bm.ReadBlock(blockPtr(n.DataPtr), it.blockBuf); err != nil {
			panic(err)
		}

		it.block = *newDataBlock(it.blockBuf)
		it.curr = it.block.Get()
	}
}

func (it *Iterator) seek(bs []byte) {
	if it.snap.db.HasBlockStore() {
		it.loadItems()
		for ; it.curr != nil && it.snap.db.keyCmp(it.curr, bs) < 0; it.curr = it.block.Get() {
		}

		if it.curr == nil {
			it.Next()
		}
	}
}

func (it *Iterator) SeekFirst() {
	it.iter.SeekFirst()
	it.skipUnwanted()
	it.loadItems()
}

func (it *Iterator) Seek(bs []byte) {
	itm := it.snap.db.newItem(bs, false)
	it.iter.SeekPrev(unsafe.Pointer(itm))
	it.skipUnwanted()
	it.seek(bs)
}

func (it *Iterator) Valid() bool {
	return it.iter.Valid()
}

func (it *Iterator) Get() []byte {
	if it.snap.db.HasBlockStore() {
		return it.curr
	}
	return (*Item)(it.iter.Get()).Bytes()
}

func (it *Iterator) GetNode() *skiplist.Node {
	return it.iter.GetNode()
}

func (it *Iterator) Next() {
	if it.curr = it.block.Get(); it.curr != nil {
		return
	}

	it.iter.Next()
	it.count++
	it.skipUnwanted()
	if it.refreshRate > 0 && it.count > it.refreshRate {
		it.Refresh()
		it.count = 0
	}
	it.loadItems()
}

// Refresh can help safe-memory-reclaimer to free deleted objects
func (it *Iterator) Refresh() {
	if it.Valid() {
		itm := it.snap.db.ptrToItem(it.GetNode().Item())
		it.iter.Close()
		it.iter = it.snap.db.store.NewIterator(it.snap.db.iterCmp, it.buf)
		it.iter.Seek(unsafe.Pointer(itm))
	}
}

func (it *Iterator) SetRefreshRate(rate int) {
	it.refreshRate = rate
}

func (it *Iterator) Close() {
	it.snap.Close()
	it.snap.db.store.FreeBuf(it.buf)
	it.iter.Close()
}

func (m *MemDB) NewIterator(snap *Snapshot) *Iterator {
	if !snap.Open() {
		return nil
	}
	buf := snap.db.store.MakeBuf()
	it := &Iterator{
		snap: snap,
		iter: m.store.NewIterator(m.iterCmp, buf),
		buf:  buf,
	}

	if snap.db.HasBlockStore() {
		it.blockBuf = make([]byte, blockSize, blockSize)
	}

	return it
}
