package platform

import (
	"container/list"
	"simple-database/internal/platform/datatype"
)

type LRU[K datatype.Scalar, V any] struct {
	list       *GenericLinkedList[K]
	dataMap    map[K]V
	elementMap map[K]*list.Element
	cap        uint32
}

func NewLRU[K datatype.Scalar, V any](cap uint32) *LRU[K, V] {
	return &LRU[K, V]{
		list:       NewLinkedList[K](),
		cap:        cap,
		dataMap:    make(map[K]V, cap),
		elementMap: make(map[K]*list.Element, cap),
	}
}

func (l *LRU[K, V]) Put(key K, val V) {
	// If the key already exists, move it to the back
	if elem, ok := l.elementMap[key]; ok {
		l.list.Remove(elem)
	} else if uint32(len(l.dataMap)) >= l.cap {
		// Evict only when inserting a new key
		l.removeLeastRecentlyUsed()
	}

	elem := l.list.PushBack(key)
	l.dataMap[key] = val
	l.elementMap[key] = elem
}

func (l *LRU[K, V]) Get(key K) V {
	val, ok := l.dataMap[key]
	if !ok {
		var zero V
		return zero
	}

	elem := l.elementMap[key]
	l.list.Remove(elem)

	newElem := l.list.PushBack(key)
	l.elementMap[key] = newElem

	return val
}

func (l *LRU[K, V]) Contains(key K) bool {
	_, ok := l.dataMap[key]
	return ok
}

func (l *LRU[K, V]) Remove(key K) {
	elem, ok := l.elementMap[key]
	if !ok {
		return
	}

	l.list.Remove(elem)
	delete(l.elementMap, key)
	delete(l.dataMap, key)
}

func (l *LRU[K, V]) removeLeastRecentlyUsed() {
	elem := l.list.Front()
	if elem == nil {
		return
	}

	key := elem.Value.(K)
	l.list.RemoveFront()
	delete(l.elementMap, key)
	delete(l.dataMap, key)
}
