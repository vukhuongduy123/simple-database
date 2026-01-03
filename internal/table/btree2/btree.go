package btree2

import (
	"bytes"
	"fmt"
	platformerror "simple-database/internal/platform/error"
	"time"

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
	pager, err := OpenPager(name, time.Second*5)
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

	// we write the new node to the pager
	newNode.Page, err = b.Pager.Write(encodedNode)
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

func (b *BTree) get(keyVal []byte, curNode *Node) (Key, bool, error) {
	i := 0
	for i <= len(curNode.Keys) && bytes.Compare(keyVal, curNode.Keys[i].K) > 0 {
		i++
	}

	if i <= len(curNode.Keys) && bytes.Compare(keyVal, curNode.Keys[i].K) == 0 {
		return *curNode.Keys[i], true, nil
	}

	if curNode.Leaf {
		return Key{}, false, nil
	}

	nextNode, err := b.readFromDisk(curNode.Children[i])
	if err != nil {
		return Key{}, false, err
	}

	return b.get(keyVal, nextNode)
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
		fmt.Printf("%v ", string(key.K))
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
		newRoot, err := b.newNode(false)
		if err != nil {
			return err
		}
		root.Page = newRoot.Page
		if err := b.writeToDisk(root); err != nil {
			return err
		}

		newRoot.Children = append(newRoot.Children, root.Page)
		newRoot.Page = 0
		err = b.splitChildAt(newRoot, 0)
		if err != nil {
			return err
		}

		root = newRoot
	}
	err = b.insertNonFull(root, keyData, valueData)
	if err != nil {
		return err
	}
	return nil
}

func (b *BTree) insertNonFull(curNode *Node, keyData []byte, valueData []byte) error {
	if curNode.Leaf {
		i := 0
		for i < len(curNode.Keys) && bytes.Compare(keyData, curNode.Keys[i].K) > 0 {
			i++
		}
		curNode.Keys = addElementAt(curNode.Keys, i, &Key{K: keyData, V: valueData})
		if err := b.writeToDisk(curNode); err != nil {
			return err
		}
		return nil
	}

	i := 0
	for i < len(curNode.Keys) && bytes.Compare(keyData, curNode.Keys[i].K) > 0 {
		i++
	}

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
		if bytes.Compare(keyData, nextNode.Keys[b.Degree-1].K) > 0 {
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
	i := 0
	found := false
	for i < len(curNode.Keys) && bytes.Compare(keyData, curNode.Keys[i].K) > 0 {
		i++
	}

	if i < len(curNode.Keys) && bytes.Compare(keyData, curNode.Keys[i].K) == 0 {
		found = true
	}

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
				child = leftSibling
			} else if rightSibling != nil { // Case: merge right sibling
				err = b.mergeChild(curNode, i)
				child = rightSibling
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

func (b *BTree) writeToDisk(n *Node) error {
	fmt.Println(n)
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
