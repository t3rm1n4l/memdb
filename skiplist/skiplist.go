package skiplist

import (
	"math/rand"
	"sync/atomic"
	"unsafe"
)

const MaxLevel = 32
const p = 0.25

type CompareFn func(unsafe.Pointer, unsafe.Pointer) int
type ItemSizeFn func(unsafe.Pointer) int

func defaultItemSize(unsafe.Pointer) int {
	return 0
}

type CreateNodeFn func(itm unsafe.Pointer, level int) *Node
type DestroyNodeFn func(*Node)

type Skiplist struct {
	head      *Node
	tail      *Node
	level     int32
	stats     stats
	usedBytes int64
	barrier   *AccessBarrier

	itemSize ItemSizeFn

	newNode  CreateNodeFn
	freeNode DestroyNodeFn
}

func New() *Skiplist {
	return NewWithMM(allocNode, nil, nil)
}

func NewWithMM(createNode CreateNodeFn,
	destroyNode DestroyNodeFn,
	callb BarrierSessionDestructor) *Skiplist {

	s := &Skiplist{
		itemSize: defaultItemSize,
		barrier:  newAccessBarrier(callb != nil, callb),
		newNode:  createNode,
		freeNode: destroyNode,
	}

	head := s.newNode(nil, MaxLevel)
	tail := s.newNode(nil, MaxLevel)

	for i := 0; i <= MaxLevel; i++ {
		head.setNext(i, tail, false)
	}

	s.head = head
	s.tail = tail

	return s
}

func (s *Skiplist) GetAccesBarrier() *AccessBarrier {
	return s.barrier
}

func (s *Skiplist) Free(n *Node) {
	s.freeNode(n)
	atomic.AddInt64(&s.stats.nodeFrees, 1)
}

func (s *Skiplist) SetItemSizeFunc(fn ItemSizeFn) {
	s.itemSize = fn
}

type ActionBuffer struct {
	preds []*Node
	succs []*Node
}

func (s *Skiplist) MakeBuf() *ActionBuffer {
	return &ActionBuffer{
		preds: make([]*Node, MaxLevel+1),
		succs: make([]*Node, MaxLevel+1),
	}
}

func (s *Skiplist) FreeBuf(b *ActionBuffer) {
}

func (s *Skiplist) Size(n *Node) int {
	return s.itemSize(n.Item()) + n.Size()
}

func (s *Skiplist) NewLevel(randFn func() float32) int {
	var nextLevel int

	for ; randFn() < p; nextLevel++ {
	}

	if nextLevel > MaxLevel {
		nextLevel = MaxLevel
	}

	level := int(atomic.LoadInt32(&s.level))
	if nextLevel > level {
		if atomic.CompareAndSwapInt32(&s.level, int32(level), int32(level+1)) {
			nextLevel = level + 1
		} else {
			nextLevel = level
		}
	}

	return nextLevel
}

func (s *Skiplist) helpDelete(level int, prev, curr, next *Node) bool {
	success := prev.dcasNext(level, curr, next, false, false)
	if success && level == curr.Level() {
		atomic.AddInt64(&s.stats.softDeletes, -1)
		atomic.AddInt64(&s.stats.levelNodesCount[level], -1)
		atomic.AddInt64(&s.usedBytes, -int64(s.Size(curr)))
	}
	return success
}

func (s *Skiplist) findPath(itm unsafe.Pointer, cmp CompareFn,
	buf *ActionBuffer) (foundNode *Node) {
	var cmpVal int = 1

retry:
	prev := s.head
	level := int(atomic.LoadInt32(&s.level))
	for i := level; i >= 0; i-- {
		curr, _ := prev.getNext(i)
	levelSearch:
		for {
			next, deleted := curr.getNext(i)
			for deleted {
				if !s.helpDelete(i, prev, curr, next) {
					atomic.AddUint64(&s.stats.readConflicts, 1)
					goto retry
				}

				curr, _ = prev.getNext(i)
				next, deleted = curr.getNext(i)
			}

			cmpVal = compare(cmp, curr.Item(), itm)
			if cmpVal < 0 {
				prev = curr
				curr, _ = prev.getNext(i)
			} else {
				break levelSearch
			}
		}

		buf.preds[i] = prev
		buf.succs[i] = curr
	}

	if cmpVal == 0 {
		foundNode = buf.succs[0]
	}
	return
}

func (s *Skiplist) Insert(itm unsafe.Pointer, cmp CompareFn, buf *ActionBuffer) (success bool) {
	_, success = s.Insert2(itm, cmp, nil, buf, rand.Float32)
	return
}

func (s *Skiplist) Insert2(itm unsafe.Pointer, inscmp CompareFn, eqCmp CompareFn,
	buf *ActionBuffer, randFn func() float32) (*Node, bool) {
	itemLevel := s.NewLevel(randFn)
	return s.Insert3(itm, inscmp, eqCmp, buf, itemLevel, false)
}

func (s *Skiplist) Insert3(itm unsafe.Pointer, insCmp CompareFn, eqCmp CompareFn,
	buf *ActionBuffer, itemLevel int, skipFindPath bool) (*Node, bool) {

	token := s.barrier.Acquire()
	defer s.barrier.Release(token)

	x := s.newNode(itm, itemLevel)

	atomic.AddInt64(&s.stats.nodeAllocs, 1)
	atomic.AddInt64(&s.stats.levelNodesCount[itemLevel], 1)
	atomic.AddInt64(&s.usedBytes, int64(s.Size(x)))

retry:
	if skipFindPath {
		skipFindPath = false
	} else {
		if s.findPath(itm, insCmp, buf) != nil {
			return nil, false
		}

		if eqCmp != nil && compare(eqCmp, itm, buf.preds[0].Item()) == 0 {
			return nil, false
		}
	}

	x.setNext(0, buf.succs[0], false)
	if !buf.preds[0].dcasNext(0, buf.succs[0], x, false, false) {
		atomic.AddUint64(&s.stats.insertConflicts, 1)
		goto retry
	}

	for i := 1; i <= int(itemLevel); i++ {
	fixThisLevel:
		for {
			x.setNext(i, buf.succs[i], false)
			if buf.preds[i].dcasNext(i, buf.succs[i], x, false, false) {
				break fixThisLevel
			}
			s.findPath(itm, insCmp, buf)
		}
	}

	return x, true
}

func (s *Skiplist) softDelete(delNode *Node) bool {
	var deleteMarked bool

	targetLevel := delNode.Level()
	for i := targetLevel; i >= 0; i-- {
		next, deleted := delNode.getNext(i)
		for !deleted {
			deleteMarked = delNode.dcasNext(i, next, next, false, true)
			next, deleted = delNode.getNext(i)
		}
	}

	if deleteMarked {
		atomic.AddInt64(&s.stats.softDeletes, 1)
	}

	return deleteMarked
}

func (s *Skiplist) Delete(itm unsafe.Pointer, cmp CompareFn, buf *ActionBuffer) bool {
	token := s.barrier.Acquire()
	defer s.barrier.Release(token)

	found := s.findPath(itm, cmp, buf) != nil
	if !found {
		return false
	}

	delNode := buf.succs[0]
	return s.deleteNode(delNode, cmp, buf)
}

func (s *Skiplist) DeleteNode(n *Node, cmp CompareFn, buf *ActionBuffer) bool {
	token := s.barrier.Acquire()
	defer s.barrier.Release(token)

	return s.deleteNode(n, cmp, buf)
}

func (s *Skiplist) deleteNode(n *Node, cmp CompareFn, buf *ActionBuffer) bool {
	itm := n.Item()
	if s.softDelete(n) {
		s.findPath(itm, cmp, buf)
		return true
	}

	return false
}

// Explicit barrier and release should be used by the caller before
// and after this function call
func (s *Skiplist) GetRangeSplitItems(nways int) []unsafe.Pointer {
	var deleted bool
repeat:
	var itms []unsafe.Pointer
	var finished bool

	l := int(atomic.LoadInt32(&s.level))
	for ; l >= 0; l-- {
		c := int(atomic.LoadInt64(&s.stats.levelNodesCount[l]) + 1)
		if c >= nways {
			perSplit := c / nways
			node := s.head
			for j := 0; node != s.tail && !finished; j++ {
				if j == perSplit {
					j = -1
					itms = append(itms, node.Item())
					finished = len(itms) == nways-1
				}

				node, deleted = node.getNext(l)
				if deleted {
					goto repeat
				}
			}

			break
		}
	}

	return itms
}
