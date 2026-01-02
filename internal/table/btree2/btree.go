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

	if i <= len(curNode.Keys) && bytes.Compare(keyVal, curNode.Keys[i-1].K) == 0 {
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

	return b.remove(root, keyData)
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

	if i < len(curNode.Keys) && bytes.Compare(keyData, curNode.Keys[i-1].K) == 0 {
		found = true
	}

	if found {
		if curNode.Leaf {
			curNode.Keys = removeAt(curNode.Keys, i)
		}

		if err := b.writeToDisk(curNode); err != nil {
			return err
		}
	} else {
		if curNode.Leaf {
			return nil
		}
	}

	return nil
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
