package main

import (
	"container/heap"
)

type sorter map[uint][]uint
type SorterWriter struct {
	data sorter
}

func MakeSorterWriter() *SorterWriter {
	return &SorterWriter{make(sorter)}
}

func (self *SorterWriter) Set(term uint, doc uint) {
	if _, ok := self.data[term]; !ok {
		self.data[term] = make([]uint, 0)
	}
	self.data[term] = append(self.data[term], doc)
}

type IndexWriter struct {
	term     uint
	doc      uint
	capacity int
	buff     []uint
	flush    IndexWriterFlush
}

type IndexWriterFlush func(iw *IndexWriter)

func MakeIndexWriter(capacity int) *IndexWriter {
	return &IndexWriter{buff: make([]uint, 0), capacity: capacity}
}

func (self *IndexWriter) Set(term uint, doc uint) {
	if len(self.buff) == self.capacity || (self.term > 0 && self.term != term) {
		self.flush(self)
	}
	self.term = term
	self.buff = append(self.buff, doc)
}

type IndexReader struct {
	term  uint
	doc   uint
	index int
}

func (self *IndexReader) Read() bool {
	// return true when eof reached
	return false
}

type PriorityQueue []*IndexReader

func (pq PriorityQueue) Len() int { return len(pq) }

func (pq PriorityQueue) Less(i, j int) bool {
	if pq[i].term == pq[j].term {
		return pq[i].doc < pq[j].doc
	} else {
		return pq[i].term < pq[j].term
	}
}

func (pq PriorityQueue) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
	pq[i].index = i
	pq[j].index = j
}

func (pq *PriorityQueue) Push(x interface{}) {
	n := len(*pq)
	item := x.(*IndexReader)
	item.index = n
	*pq = append(*pq, item)
}

func (pq *PriorityQueue) Pop() interface{} {
	old := *pq
	n := len(old)
	item := old[n-1]
	item.index = -1 // for safety
	*pq = old[0 : n-1]
	return item
}

// update modifies the priority and value of an Item in the queue.
func (pq *PriorityQueue) update(item *IndexReader) {
	heap.Fix(pq, item.index)
}

func Merger(readers []*IndexReader, writer *IndexWriter) {
	pq := make(PriorityQueue, len(readers))

	for k, ent := range readers {
		ent.index = k
		pq[k] = ent
	}
	heap.Init(&pq)

	for pq.Len() > 0 {
		ent := pq[0]
		if eof := ent.Read(); eof {
			pq.Pop()
			continue
		}
		writer.Set(ent.term, ent.doc)
	}
}

func main() {

	sw := MakeSorterWriter()
	for k := 0; k < 10000000; k++ {
		sw.Set(uint(k%1000), uint(k))
	}

	iw := MakeIndexWriter(10)
	for k := 0; k < 1000; k++ {
		iw.Set(uint(1), uint(k))
	}
}
