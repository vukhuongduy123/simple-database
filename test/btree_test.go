package test

import (
	"encoding/binary"
	"fmt"
	"os"
	"simple-database/internal/engine/table/btree"
	"testing"
)

func TestBree(t *testing.T) {
	_ = os.RemoveAll("data/test")
	_ = os.MkdirAll("data/test", 0777)
	// Initialize BTree
	b, err := btree.Open("data/test/mydb")
	if err != nil {
		t.Fatalf("Failed to open btree: %v", err)
	}

	// Ensure closure at the end of the test
	defer func() {
		if err := b.Close(); err != nil {
			t.Errorf("Failed to close btree: %v", err)
		}
	}()

	// 1. Insert 1000 keys
	for i := 0; i < 1000; i++ {
		buf := make([]byte, 8)
		binary.BigEndian.PutUint64(buf, uint64(i))
		if err := b.Insert(buf, buf); err != nil {
			t.Fatalf("Insert failed at index %d: %v", i, err)
		}
	}

	// 2. Verify Size
	size, err := b.Size()
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println("Total B-Tree Size:", size)
	if size != 1000 {
		t.Errorf("Expected size to be 1000, got %d", size)
	}

	// 3. Test Range Queries (Less Than / Greater Than)
	for i := 0; i < 1000; i++ {
		buf := make([]byte, 8)
		binary.BigEndian.PutUint64(buf, uint64(i))

		// LessThanOrEqual
		ltEq, _ := b.LessThanOrEqual(buf)
		if len(ltEq) != i+1 {
			t.Errorf("Expected LessThanOrEqual(%d) to be %d, got %d", i, i+1, len(ltEq))
		}

		// LessThan
		lt, _ := b.LessThan(buf)
		if len(lt) != i {
			t.Errorf("Expected LessThan(%d) to be %d, got %d", i, i, len(lt))
		}

		// GreaterThanOrEqual
		gtEq, _ := b.GreaterThanOrEqual(buf)
		if len(gtEq) != 1000-i {
			t.Errorf("Expected GreaterThanOrEqual(%d) to be %d, got %d", i, 1000-i, len(gtEq))
		}

		// GreaterThan
		gt, _ := b.GreaterThan(buf)
		if len(gt) != 1000-i-1 {
			t.Errorf("Expected GreaterThan(%d) to be %d, got %d", i, 1000-i-1, len(gt))
		}
	}

	// 4. Test Point Lookups (Get)
	for i := 0; i < 1000; i++ {
		buf := make([]byte, 8)
		binary.BigEndian.PutUint64(buf, uint64(i))
		_, found, err := b.Get(buf)
		if err != nil {
			t.Errorf("Failed to Get key %d: %v", i, err)
		}
		if !found {
			t.Errorf("Expected to find key %d", i)
		}
	}

}
