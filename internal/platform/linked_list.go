package platform

import (
	"container/list"
)

type (
	GenericLinkedList[T any] struct {
		list *list.List
	}
)

func NewLinkedList[T any]() *GenericLinkedList[T] {
	return &GenericLinkedList[T]{list: list.New()}
}

func (l *GenericLinkedList[T]) PushBack(val T) *list.Element {
	return l.list.PushBack(val)
}

func (l *GenericLinkedList[T]) Remove(val *list.Element) {
	l.list.Remove(val)
}

func (l *GenericLinkedList[T]) RemoveFront() T {
	return l.list.Remove(l.list.Front()).(T)
}

func (l *GenericLinkedList[T]) Front() *list.Element {
	return l.list.Front()
}
