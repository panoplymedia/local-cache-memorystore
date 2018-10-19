package priorityqueue

import (
	"container/heap"
)

// Based on priorityqueue example from https://golang.org/pkg/container/heap/

// An Item is something we manage in a priority queue.
type Item struct {
	Value    string // The value of the item
	Priority int64  // The priority of the item in the queue.
	// The index is needed by update and is maintained by the heap.Interface methods.
	index int // The index of the item in the heap.
}

// A PriorityQueue implements heap.Interface and holds Items.
type PriorityQueue []*Item

// Len returns the length of the PriorityQueue
func (pq PriorityQueue) Len() int { return len(pq) }

// Less compares items in PriorityQueue
func (pq PriorityQueue) Less(i, j int) bool {
	// sort by lowest priority first
	return pq[i].Priority < pq[j].Priority
}

// Swap swaps items in a PriorityQueue
func (pq PriorityQueue) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
	pq[i].index = i
	pq[j].index = j
}

// Push puts an item onto the PriorityQueue
func (pq *PriorityQueue) Push(x interface{}) {
	n := len(*pq)
	item := x.(*Item)
	item.index = n
	*pq = append(*pq, item)
}

// Pop removes and returns the top item in the PriorityQueue
func (pq *PriorityQueue) Pop() interface{} {
	old := *pq
	n := len(old)
	if n < 1 {
		return nil
	}
	item := old[0]
	item.index = -1 // for safety
	*pq = old[1:]
	return item
}

// Peek returns the top item without removing it
func (pq *PriorityQueue) Peek() interface{} {
	n := len(*pq)
	if n < 1 {
		return nil
	}
	item := (*pq)[0]
	return item
}

// Update modifies the priority and value of an Item in the PriorityQueue
func (pq *PriorityQueue) Update(item *Item, value string, priority int64) {
	item.Value = value
	item.Priority = priority
	heap.Fix(pq, item.index)
}
