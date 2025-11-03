package platform

import (
	"fmt"
	"simple-database/internal/platform/datatype"
	errors "simple-database/internal/platform/error"
)

type LRU[K datatype.Scalar, V any] struct {
	list    *LinkedList[K]
	dataMap map[K]V
	cap     int
	len     int
}

func NewLRU[K datatype.Scalar, V any](cap int, equals EqFunc[K]) *LRU[K, V] {
	return &LRU[K, V]{
		list:    NewLinkedList[K](equals),
		cap:     cap,
		dataMap: make(map[K]V),
	}
}

func (l *LRU[K, V]) Put(key K, val V) error {
	if l.len >= l.cap {
		if err := l.removeLeastRecentlyUsed(); err != nil {
			return fmt.Errorf("lru.Put: %w", err)
		}
	}

	l.list.Append(key)
	l.dataMap[key] = val
	l.len++

	return nil
}

func (l *LRU[T, K]) removeLeastRecentlyUsed() error {
	node, err := l.list.RemoveByIdx(0)
	if err != nil {
		return fmt.Errorf("lru.removeLeastRecentlyUsed: %w", err)
	}
	l.len--
	delete(l.dataMap, node.val)
	return nil
}

func (l *LRU[K, V]) Get(key K) (V, error) {
	var zero V
	val, ok := l.dataMap[key]
	if !ok {
		return zero, errors.NewItemNotInLinkedListError(l.dataMap, key)
	}
	err := l.list.Remove(key)
	if err != nil {
		return zero, fmt.Errorf("lru.Get: %w", err)
	}
	l.list.Append(key)
	return val, nil
}

func (l *LRU[K, V]) Remove(key K) error {
	_, ok := l.dataMap[key]
	if !ok {
		return errors.NewItemNotInLinkedListError(l.dataMap, key)
	}
	delete(l.dataMap, key)
	return l.list.Remove(key)
}
