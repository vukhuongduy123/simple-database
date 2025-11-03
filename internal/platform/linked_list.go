package platform

import errors "simple-database/internal/platform/error"

type (
	EqFunc[T any] func(a, b T) bool

	Node[T any] struct {
		val  T
		next *Node[T]
		prev *Node[T]
	}

	LinkedList[T any] struct {
		head   *Node[T]
		tail   *Node[T]
		count  int
		equals EqFunc[T]
	}
)

func NewLinkedList[T any](equals EqFunc[T]) *LinkedList[T] {
	return &LinkedList[T]{
		head:   nil,
		tail:   nil,
		equals: equals,
	}
}

func (l *LinkedList[T]) Count() int {
	return l.count
}

func (l *LinkedList[T]) Append(val T) {
	node := &Node[T]{
		val: val,
	}
	if l.head == nil {
		l.head = node
		l.tail = node
	} else {
		node.prev = l.tail
		l.tail.next = node
		l.tail = node
	}
	l.count++
}

func (l *LinkedList[T]) Remove(val T) error {
	node, err := l.Find(val)
	if err != nil {
		return err
	}

	if node == l.head {
		if l.count == 1 {
			l.head = nil
			l.tail = nil
			l.count = 0
			return nil
		}
		l.head.next.prev = nil
		l.head = l.head.next
	} else if node == l.tail {
		l.tail.prev.next = nil
		l.tail = node.prev
	} else {
		node.prev.next = node.next
		node.next.prev = node.prev
	}

	l.count--
	return nil
}

func (l *LinkedList[T]) Find(val T) (*Node[T], error) {
	node := l.head
	for node != nil {
		if l.equals(node.val, val) {
			return node, nil
		}
		node = node.next
	}
	return nil, errors.NewItemNotInLinkedListError(l, val)
}

func (l *LinkedList[T]) FindByIdx(idx int) (*Node[T], error) {
	node := l.head
	i := 0
	for node != nil {
		if i == idx {
			return node, nil
		}
		i++
		node = node.next
	}
	return nil, errors.NewItemNotInLinkedListError(l, idx)
}

func (l *LinkedList[T]) RemoveByIdx(idx int) (*Node[T], error) {
	node, err := l.FindByIdx(idx)
	if err != nil {
		return nil, err
	}

	if node == l.head {
		if l.count == 1 {
			l.head = nil
			l.tail = nil
			l.count = 0
			return node, nil
		}
		l.head.next.prev = nil
		l.head = l.head.next
	} else if node == l.tail {
		l.tail.prev.next = nil
		l.tail = node.prev
	} else {
		node.prev.next = node.next
		node.next.prev = node.prev
	}

	l.count--
	return node, nil
}

func (l *LinkedList[T]) Values() []T {
	values := make([]T, 0)
	node := l.head
	for node != nil {
		values = append(values, node.val)
		node = node.next
	}
	return values
}
