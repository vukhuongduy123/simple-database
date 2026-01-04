package btree

import (
	"bytes"
	"fmt"
	platformerror "simple-database/internal/platform/error"

	"github.com/hashicorp/go-msgpack/codec"
)

type BTree struct {
	Pager  *Pager // The pager for the btree
	Degree int    // The order of the tree
}

type Key struct {
	// TODO: add unique constraint to show that the key is unique for non-unique index
	K []byte // The key
	V []byte // The values
}

type Node struct {
	Page     int64   // The page number of the node
	Keys     []*Key  // The keys in the node
	Children []int64 // The children of the node
	Leaf     bool    // If the node is a leaf node
}

func (k *Key) String() string {
	return fmt.Sprintf("key: %v", k.K)
}

func (n *Node) String() string {
	return fmt.Sprintf("page: %d, keys: %v, children: %v, leaf: %v", n.Page, n.Keys, n.Children, n.Leaf)
}

const DefaultDegree = 2

// Open opens a new or existing BTree
func Open(name string) (*BTree, error) {
	pager, err := OpenPager(name)
	if err != nil {
		return nil, err
	}

	return &BTree{Degree: DefaultDegree, Pager: pager}, nil
}

// Close closes the BTree
func (b *BTree) Close() error {
	return b.Pager.Close()
}

// encodeNode encodes a node into a byte slice
func encodeNode(n *Node) ([]byte, error) {
	// Create a new msgpack handle
	handle := new(codec.MsgpackHandle)

	var encoded []byte
	enc := codec.NewEncoderBytes(&encoded, handle)
	err := enc.Encode(n)
	if err != nil {
		return nil, platformerror.NewStackTraceError(err.Error(), platformerror.BinaryWriteErrorCode)
	}

	if len(encoded) > pageSize {
		return nil, platformerror.NewStackTraceError(fmt.Sprintf("Node size %d exceed %d", len(encoded), pageSize),
			platformerror.BTreeWriteError)
	}

	return encoded, nil
}

// newNode creates a new BTree node
func (b *BTree) newNode(leaf bool) (*Node, error) {
	var err error

	newNode := &Node{
		Leaf:     leaf,
		Keys:     make([]*Key, 0),
		Children: make([]int64, 0),
	}

	// we encode the new node
	encodedNode, err := encodeNode(newNode)
	if err != nil {
		return nil, err
	}

	newNode.Page, err = b.Pager.NextPageId()
	if err != nil {
		return nil, err
	}

	encodedNode, err = encodeNode(newNode)
	if err != nil {
		return nil, err
	}

	// Write an updated node
	err = b.Pager.WriteTo(newNode.Page, encodedNode)
	if err != nil {
		return nil, err
	}

	// we return the new node
	return newNode, nil
}

// decodeNode decodes a byte slice into a node
func decodeNode(data []byte) (*Node, error) {
	// Create a new msgpack handle
	handle := new(codec.MsgpackHandle)

	var n *Node

	dec := codec.NewDecoderBytes(data, handle)
	err := dec.Decode(&n)
	if err != nil {
		return nil, platformerror.NewStackTraceError(err.Error(), platformerror.BinaryReadErrorCode)
	}

	return n, nil
}

// getRoot returns the root of the BTree
func (b *BTree) getRoot() (*Node, error) {
	root, err := b.Pager.GetPage(0)
	if err != nil {
		if err.Error() == "EOF" {
			// create root
			// initial root if a leaf node and starts at page 0
			rootNode := &Node{
				Leaf:     true,
				Page:     0,
				Children: make([]int64, 0),
				Keys:     make([]*Key, 0),
			}

			// encode the root node
			encodedRoot, err := encodeNode(rootNode)
			if err != nil {
				return nil, err
			}

			// write the root to the file
			err = b.Pager.WriteTo(0, encodedRoot)
			if err != nil {
				return nil, err
			}

			if err := b.writeToDisk(rootNode); err != nil {
				return nil, err
			}

			return rootNode, nil
		}

		return nil, err
	}

	// decode the root
	rootNode, err := decodeNode(root)
	if err != nil {
		return nil, err
	}

	return rootNode, nil
}

func (b *BTree) Get(keyVal []byte) (Key, bool, error) {
	if keyVal == nil {
		return Key{}, false, platformerror.NewStackTraceError("keyVal cannot be nil", platformerror.BTreeReadError)
	}

	root, err := b.getRoot()
	if err != nil {
		return Key{}, false, err
	}
	return b.get(keyVal, root)
}

func (n *Node) search(key []byte) (int, bool) {
	low, high := 0, len(n.Keys)
	var mid int
	for low < high {
		mid = (low + high) / 2
		cmp := bytes.Compare(key, n.Keys[mid].K)
		switch {
		case cmp > 0:
			low = mid + 1
		case cmp < 0:
			high = mid
		default:
			return mid, true
		}
	}
	return low, false
}

func (b *BTree) get(keyVal []byte, n *Node) (Key, bool, error) {
	for next := n; next != nil; {
		i, found := next.search(keyVal)
		if found {
			return *n.Keys[i], true, nil
		}
		if !next.Leaf {
			nextNode, err := b.readFromDisk(next.Children[0])
			if err != nil {
				return Key{}, false, err
			}
			next = nextNode
		}
	}
	return Key{}, false, nil
}

// From
// x
// |
// y
// To
//  x
// |  |
// y  z

func (b *BTree) splitChildAt(x *Node, i int32) error {
	y, err := b.readFromDisk(x.Children[i])
	if err != nil {
		return err
	}

	z, err := b.newNode(y.Leaf)
	if err != nil {
		return err
	}
	z.Keys = append(z.Keys, y.Keys[b.Degree:]...)

	if !y.Leaf {
		z.Children = append(z.Children, y.Children[b.Degree:]...)
		y.Children = y.Children[:b.Degree]
	}

	x.Children = addElementAt(x.Children, int(i+1), z.Page)
	x.Keys = addElementAt(x.Keys, int(i), y.Keys[b.Degree-1])

	y.Keys = y.Keys[:b.Degree-1]

	// encode y
	if err := b.writeToDisk(y); err != nil {
		return err
	}

	// encode z
	if err := b.writeToDisk(z); err != nil {
		return err
	}

	// encode x
	if err := b.writeToDisk(x); err != nil {
		return err
	}

	return nil
}

func addElementAt[T any](s []T, u int, v T) []T {
	if u < 0 || u > len(s) {
		panic("index out of range")
	}

	var zero T
	s = append(s, zero)  // grow slice
	copy(s[u+1:], s[u:]) // shift right
	s[u] = v

	return s
}

func (b *BTree) PrintTree() error {
	root, err := b.getRoot()
	if err != nil {
		return err
	}
	err = b.printTree(root, "", true)
	if err != nil {
		return err
	}
	return nil
}

func (b *BTree) printTree(node *Node, indent string, last bool) error {
	fmt.Print(indent)
	if last {
		fmt.Print("└── ")
		indent += "    "
	} else {
		fmt.Print("├── ")
		indent += "│   "
	}

	for _, key := range node.Keys {
		fmt.Printf("%v ", key.K)
	}
	fmt.Println()

	for i, child := range node.Children {
		c, err := b.readFromDisk(child)
		if err != nil {
			return err
		}

		err = b.printTree(c, indent, i == len(node.Children)-1)
		if err != nil {
			return err
		}
	}

	return nil
}

func (b *BTree) Insert(keyData []byte, valueData []byte) error {
	root, err := b.getRoot()
	if err != nil {
		return err
	}
	if len(root.Keys) == 2*b.Degree-1 {
		// 1. Create a new node to hold the old root's data
		oldRootCopy, err := b.newNode(root.Leaf)
		if err != nil {
			return err
		}

		// 2. Move data from Page 0 to the new page
		oldRootCopy.Keys = root.Keys
		oldRootCopy.Children = root.Children
		if err := b.writeToDisk(oldRootCopy); err != nil {
			return err
		}

		// 3. Clear Page 0 to become the new parent (the new root)
		root.Keys = make([]*Key, 0)
		root.Children = []int64{oldRootCopy.Page}
		root.Leaf = false // Root is no longer a leaf
		if err := b.writeToDisk(root); err != nil {
			return err
		}

		// 4. Split the "new" child (the old data)
		err = b.splitChildAt(root, 0)
		if err != nil {
			return err
		}
	}
	err = b.insertNonFull(root, keyData, valueData)
	if err != nil {
		return err
	}
	return nil
}

func (b *BTree) insertNonFull(curNode *Node, keyData []byte, valueData []byte) error {
	if curNode.Leaf {
		i, found := curNode.search(keyData)
		if found {
			return platformerror.NewStackTraceError("Key is duplicate", platformerror.BinaryWriteErrorCode)
		}

		curNode.Keys = addElementAt(curNode.Keys, i, &Key{K: keyData, V: valueData})
		if err := b.writeToDisk(curNode); err != nil {
			return err
		}
		return nil
	}

	i, _ := curNode.search(keyData)

	nextNode, err := b.readFromDisk(curNode.Children[i])
	if err != nil {
		return err
	}
	if len(nextNode.Keys) == 2*b.Degree-1 {
		err = b.splitChildAt(curNode, int32(i))
		if err != nil {
			return err
		}
		// determine which of the two children is now the correct one to descend to
		if bytes.Compare(keyData, curNode.Keys[i].K) > 0 {
			i++
		}

		nextNode, err = b.readFromDisk(curNode.Children[i])
		if err != nil {
			return err
		}
	}

	err = b.insertNonFull(nextNode, keyData, valueData)
	if err != nil {
		return err
	}
	return nil
}

func (b *BTree) Remove(keyData []byte) error {
	root, err := b.getRoot()
	if err != nil {
		return err
	}

	err = b.remove(root, keyData)
	if err != nil {
		return err
	}

	root, err = b.getRoot()
	if err != nil {
		return err
	}

	if len(root.Keys) == 0 && !root.Leaf {
		// Root has exactly one child
		child, err := b.readFromDisk(root.Children[0])
		if err != nil {
			return err
		}

		// Copy child's CONTENT into root (page 0)
		root.Keys = child.Keys
		root.Children = child.Children
		root.Leaf = child.Leaf

		// Persist updated root
		return b.writeToDisk(root)
	}

	return nil
}

// RemoveAt removes the element at index i from slice s.
// It panics if i is out of range (same behavior as built-in operations).
func removeAt[T any](s []T, i int) []T {
	return append(s[:i], s[i+1:]...)
}

func (b *BTree) remove(curNode *Node, keyData []byte) error {
	i, found := curNode.search(keyData)

	if found {
		// Case: node is a left
		if curNode.Leaf {
			curNode.Keys = removeAt(curNode.Keys, i)
			if err := b.writeToDisk(curNode); err != nil {
				return err
			}
		} else { // Case: node is an internal node
			leftChild, err := b.readFromDisk(curNode.Children[i])
			if err != nil {
				return err
			}
			// Case: left child has at least t keys
			if len(leftChild.Keys) >= b.Degree {
				predecessorNode, err := b.getPredecessor(leftChild)
				if err != nil {
					return err
				}
				predecessorKey := predecessorNode.Keys[len(predecessorNode.Keys)-1]
				curNode.Keys[i] = predecessorKey
				if err := b.writeToDisk(curNode); err != nil {
					return err
				}

				return b.remove(leftChild, predecessorKey.K)
			}

			rightChild, err := b.readFromDisk(curNode.Children[i+1])
			if err != nil {
				return err
			}
			// Case: right child has at least t keys
			if len(rightChild.Keys) >= b.Degree {
				successorNode, err := b.getSuccessor(rightChild)
				if err != nil {
					return err
				}
				successorKey := successorNode.Keys[0]
				curNode.Keys[i] = successorKey
				if err := b.writeToDisk(curNode); err != nil {
					return err
				}

				return b.remove(rightChild, successorKey.K)
			}

			// Case: both children have t-1 keys
			err = b.mergeChild(curNode, i)
			if err != nil {
				return err
			}
			mergedChild, err := b.readFromDisk(curNode.Children[i])
			if err != nil {
				return err
			}
			return b.remove(mergedChild, keyData)
		}
	} else {
		if curNode.Leaf {
			return nil
		}
		child, err := b.readFromDisk(curNode.Children[i])
		if err != nil {
			return err
		}

		// if a child has only t-1 keys, we need to ensure it has at least t keys
		if len(child.Keys) == b.Degree-1 {
			var leftSibling *Node = nil
			if i > 0 {
				leftSibling, err = b.readFromDisk(curNode.Children[i-1])
				if err != nil {
					return err
				}
			}

			var rightSibling *Node = nil
			if i < len(curNode.Children)-1 {
				rightSibling, err = b.readFromDisk(curNode.Children[i+1])
				if err != nil {
					return err
				}
			}

			// Case: borrow from left sibling
			if leftSibling != nil && len(leftSibling.Keys) >= b.Degree {
				// Move parent's separator key down to child (front)
				child.Keys = addElementAt(child.Keys, 0, curNode.Keys[i-1])

				// If internal node, move a rightmost child pointer
				if !leftSibling.Leaf {
					child.Children = addElementAt(child.Children, 0, leftSibling.Children[len(leftSibling.Children)-1])
					leftSibling.Children = leftSibling.Children[:len(leftSibling.Children)-1]
				}

				// Move left sibling's rightmost key up to parent
				curNode.Keys[i-1] = leftSibling.Keys[len(leftSibling.Keys)-1]

				// Remove key from left sibling
				leftSibling.Keys = leftSibling.Keys[:len(leftSibling.Keys)-1]
				if err := b.writeToDisk(curNode); err != nil {
					return err
				}
				if err := b.writeToDisk(leftSibling); err != nil {
					return err
				}
				if err := b.writeToDisk(child); err != nil {
					return err
				}
			} else if rightSibling != nil && len(rightSibling.Keys) >= b.Degree { // Case: borrow from the right sibling
				// Move parent's separator key down to the child (append at the end)
				child.Keys = append(child.Keys, curNode.Keys[i])

				// Replace parent's separator with right sibling's first key
				curNode.Keys[i] = rightSibling.Keys[0]

				// Remove the borrowed key from the right sibling
				rightSibling.Keys = rightSibling.Keys[1:]

				// If internal node, move the first child pointer from the right sibling
				if !rightSibling.Leaf {
					child.Children = append(child.Children, rightSibling.Children[0])
					rightSibling.Children = rightSibling.Children[1:]
				}
				if err := b.writeToDisk(curNode); err != nil {
					return err
				}
				if err := b.writeToDisk(rightSibling); err != nil {
					return err
				}
				if err := b.writeToDisk(child); err != nil {
					return err
				}
			} else if leftSibling != nil { // Case: merge left siblings
				err = b.mergeChild(curNode, i-1)
				// Get the updated merged node
				child, err = b.readFromDisk(curNode.Children[i-1])
				if err != nil {
					return err
				}
			} else if rightSibling != nil { // Case: merge right sibling
				err = b.mergeChild(curNode, i)

				child, err = b.readFromDisk(curNode.Children[i])
				if err != nil {
					return err
				}
			}
		}
		return b.remove(child, keyData)
	}

	return nil
}

func (b *BTree) mergeChild(x *Node, i int) error {
	y, err := b.readFromDisk(x.Children[i]) // left child
	if err != nil {
		return err
	}
	z, err := b.readFromDisk(x.Children[i+1]) // right child
	if err != nil {
		return err
	}

	// 1. Move the separator key from parent into y
	y.Keys = append(y.Keys, x.Keys[i])

	// 2. Append all keys from z into y
	y.Keys = append(y.Keys, z.Keys...)

	// 3. Append children from z if internal node
	if !y.Leaf {
		y.Children = append(y.Children, z.Children...)
	}

	// 4. Remove key i from parent x
	x.Keys = removeAt(x.Keys, i)

	// 5. Remove child z (i+1) from parent x
	x.Children = removeAt(x.Children, i+1)

	if err = b.writeToDisk(y); err != nil {
		return err
	}
	if err = b.writeToDisk(x); err != nil {
		return err
	}

	return nil
}

func (b *BTree) getPredecessor(n *Node) (*Node, error) {
	currentNode := n
	for !currentNode.Leaf {
		nextNode, err := b.readFromDisk(currentNode.Children[len(currentNode.Children)-1])
		if err != nil {
			return nil, err
		}
		currentNode = nextNode
	}
	return currentNode, nil
}

func (b *BTree) getSuccessor(n *Node) (*Node, error) {
	currentNode := n
	for !currentNode.Leaf {
		nextNode, err := b.readFromDisk(currentNode.Children[0])
		if err != nil {
			return nil, err
		}
		currentNode = nextNode
	}
	return currentNode, nil
}

func (b *BTree) Size() (int64, error) {
	root, err := b.getRoot()
	if err != nil {
		return 0, err
	}
	stack := []*Node{root}
	count := 0

	for len(stack) > 0 {
		// pop
		n := stack[len(stack)-1]
		stack = stack[:len(stack)-1]

		count += len(n.Keys)

		// push children
		if !n.Leaf {
			for _, childID := range n.Children {
				child, err := b.readFromDisk(childID)
				if err != nil {
					return 0, err
				}
				stack = append(stack, child)
			}
		}
	}
	return int64(count), nil
}

func (b *BTree) writeToDisk(n *Node) error {
	encodedCurNode, err := encodeNode(n)
	if err != nil {
		return err
	}
	err = b.Pager.WriteTo(n.Page, encodedCurNode)
	if err != nil {
		return err
	}
	return nil
}

func (b *BTree) readFromDisk(pageId int64) (*Node, error) {
	data, err := b.Pager.GetPage(pageId)
	if err != nil {
		return nil, err
	}
	return decodeNode(data)
}

func (b *BTree) LessThan(keyData []byte) ([]Key, error) {
	root, err := b.getRoot()
	if err != nil {
		return nil, err
	}
	return b.lessThan(keyData, root)
}

func (b *BTree) lessThan(keyData []byte, n *Node) ([]Key, error) {
	// Currently, in the next recursive work, a search is not needed as from previous pos, the b-tree guarantee that key[pos] < keyData
	pos, _ := n.search(keyData)

	keys := make([]Key, 0)

	for i := 0; i < pos; i++ {
		if !n.Leaf {
			k, err := b.readFromDisk(n.Children[i])
			if err != nil {
				return nil, err
			}
			childKeys, err := b.lessThan(keyData, k)
			if err != nil {
				return nil, err
			}
			keys = append(keys, childKeys...)
		}
		keys = append(keys, *n.Keys[i])
	}

	if !n.Leaf {
		k, err := b.readFromDisk(n.Children[pos])
		if err != nil {
			return nil, err
		}
		childKeys, err := b.lessThan(keyData, k)
		if err != nil {
			return nil, err
		}
		keys = append(keys, childKeys...)
	}

	return keys, nil
}

func (b *BTree) LessThanOrEqual(keyData []byte) ([]Key, error) {
	root, err := b.getRoot()
	if err != nil {
		return nil, err
	}
	return b.lessThanOrEqual(keyData, root)
}

func (b *BTree) lessThanOrEqual(keyData []byte, n *Node) ([]Key, error) {
	// Currently, in the next recursive work, a search is not needed as from previous pos, the b-tree guarantee that key[pos] < keyData
	pos, found := n.search(keyData)
	if found {
		pos++
	}

	keys := make([]Key, 0)

	for i := 0; i < pos; i++ {
		if !n.Leaf {
			k, err := b.readFromDisk(n.Children[i])
			if err != nil {
				return nil, err
			}
			childKeys, err := b.lessThanOrEqual(keyData, k)
			if err != nil {
				return nil, err
			}
			keys = append(keys, childKeys...)
		}
		keys = append(keys, *n.Keys[i])
	}

	if !n.Leaf {
		k, err := b.readFromDisk(n.Children[pos])
		if err != nil {
			return nil, err
		}
		childKeys, err := b.lessThanOrEqual(keyData, k)
		if err != nil {
			return nil, err
		}
		keys = append(keys, childKeys...)
	}

	return keys, nil
}

func (b *BTree) GreaterThan(keyData []byte) ([]Key, error) {
	root, err := b.getRoot()
	if err != nil {
		return nil, err
	}

	return b.greaterThan(keyData, root)
}

func (b *BTree) greaterThan(keyData []byte, n *Node) ([]Key, error) {
	// Currently, in the next recursive work, a search is not needed as from previous pos, the b-tree guarantee that key[pos] ? keyData
	pos, found := n.search(keyData)
	if found {
		pos++
	}

	keys := make([]Key, 0)

	for i := pos; i < len(n.Keys); i++ {
		if !n.Leaf {
			k, err := b.readFromDisk(n.Children[i])
			if err != nil {
				return nil, err
			}
			childKeys, err := b.greaterThan(keyData, k)
			if err != nil {
				return nil, err
			}
			keys = append(keys, childKeys...)
		}
		keys = append(keys, *n.Keys[i])
	}

	if !n.Leaf {
		k, err := b.readFromDisk(n.Children[len(n.Children)-1])
		if err != nil {
			return nil, err
		}
		childKeys, err := b.greaterThan(keyData, k)
		if err != nil {
			return nil, err
		}
		keys = append(keys, childKeys...)
	}

	return keys, nil
}

func (b *BTree) GreaterThanOrEqual(keyData []byte) ([]Key, error) {
	root, err := b.getRoot()
	if err != nil {
		return nil, err
	}

	return b.greaterThanOrEqual(keyData, root)
}

func (b *BTree) greaterThanOrEqual(keyData []byte, n *Node) ([]Key, error) {
	// Currently, in the next recursive work, a search is not needed as from previous pos, the b-tree guarantee that key[pos] > keyData
	pos, _ := n.search(keyData)

	keys := make([]Key, 0)

	for i := pos; i < len(n.Keys); i++ {
		if !n.Leaf {
			k, err := b.readFromDisk(n.Children[i])
			if err != nil {
				return nil, err
			}
			childKeys, err := b.greaterThanOrEqual(keyData, k)
			if err != nil {
				return nil, err
			}
			keys = append(keys, childKeys...)
		}
		keys = append(keys, *n.Keys[i])
	}

	if !n.Leaf {
		k, err := b.readFromDisk(n.Children[len(n.Children)-1])
		if err != nil {
			return nil, err
		}
		childKeys, err := b.greaterThanOrEqual(keyData, k)
		if err != nil {
			return nil, err
		}
		keys = append(keys, childKeys...)
	}

	return keys, nil
}
