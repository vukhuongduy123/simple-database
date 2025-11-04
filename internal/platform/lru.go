package platform

import (
	"container/list"
	"simple-database/internal/platform/datatype"
)

type LRU[K datatype.Scalar, V any] struct {
	list       *GenericLinkedList[K]
	dataMap    map[K]V
	elementMap map[K]*list.Element
	cap        int
}

func NewLRU[K datatype.Scalar, V any](cap int) *LRU[K, V] {
	return &LRU[K, V]{
		list:       NewLinkedList[K](),
		cap:        cap,
		dataMap:    make(map[K]V),
		elementMap: make(map[K]*list.Element),
	}
}

func (l *LRU[K, V]) Put(key K, val V) error {
	if len(l.dataMap) >= l.cap {
		l.removeLeastRecentlyUsed()
	}

	v := l.list.PushBack(key)
	l.dataMap[key] = val
	l.elementMap[key] = v

	return nil
}

func (l *LRU[K, V]) removeLeastRecentlyUsed() {
	val := l.list.Front()
	if val == nil {
		return
	}
	delete(l.elementMap, val.Value.(K))
	delete(l.dataMap, val.Value.(K))
	l.list.RemoveFront()
}

func (l *LRU[K, V]) Get(key K) V {
	var zero V
	val, ok := l.dataMap[key]
	if !ok {
		return zero
	}
	elementKey := l.elementMap[key]
	l.list.Remove(elementKey)
	l.list.PushBack(key)
	return val
}

func (l *LRU[K, V]) Contains(key K) bool {
	_, ok := l.dataMap[key]
	return ok
}

func (l *LRU[K, V]) Remove(key K) {
	_, ok := l.dataMap[key]
	if !ok {
		return
	}

	elementVal := l.elementMap[key]
	delete(l.dataMap, key)
	delete(l.elementMap, key)
	l.list.Remove(elementVal)
}
