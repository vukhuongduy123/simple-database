package index

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"os"
	"simple-database/internal/platform/datatype"
	errors "simple-database/internal/platform/error"
	platformparser "simple-database/internal/platform/parser"

	"github.com/guycipher/btree"
)

type Index struct {
	btree *btree.BTree
}

// Item TODO: support for multiple types
type Item struct {
	val     int64
	PagePos int64
}

func NewItem(val, pagePos int64) *Item {
	return &Item{
		val:     val,
		PagePos: pagePos,
	}
}

func NewIndex(f string) *Index {
	bt, _ := btree.Open(f, os.O_CREATE|os.O_RDWR, 0644, 3)

	return &Index{btree: bt}
}

func (i *Item) MarshalBinary() ([]byte, error) {
	buf := bytes.Buffer{}
	// type
	if err := binary.Write(&buf, binary.LittleEndian, datatype.TypeIndexItem); err != nil {
		return nil, fmt.Errorf("Item.MarshalBinary: type: %w", err)
	}
	// len
	if err := binary.Write(&buf, binary.LittleEndian, uint32(2*(datatype.LenInt64+datatype.LenMeta))); err != nil {
		return nil, fmt.Errorf("Item.MarshalBinary: len: %w", err)
	}

	valTLV := platformparser.NewTLVMarshaler(i.val)
	valBuf, err := valTLV.MarshalBinary()
	if err != nil {
		return nil, fmt.Errorf("Item.MarshalBinary: ID TLV: %w", err)
	}
	buf.Write(valBuf)

	pagePosTLV := platformparser.NewTLVMarshaler(i.PagePos)
	pagePosBuf, err := pagePosTLV.MarshalBinary()
	if err != nil {
		return nil, fmt.Errorf("Item.MarshalBinary: page pos: %w", err)
	}
	buf.Write(pagePosBuf)
	return buf.Bytes(), nil
}

func (i *Item) UnmarshalBinary(buf []byte) error {
	byteUnmarshaler := platformparser.NewValueUnmarshaler[byte]()
	int32Unmarshaler := platformparser.NewValueUnmarshaler[uint32]()
	int64Unmarshaler := platformparser.NewValueUnmarshaler[int64]()

	n := 0

	// type
	if err := byteUnmarshaler.UnmarshalBinary(buf); err != nil {
		return fmt.Errorf("Item.MarshalBinary: type: %w", err)
	}
	n += datatype.LenByte

	// len
	if err := int32Unmarshaler.UnmarshalBinary(buf[n:]); err != nil {
		return fmt.Errorf("Item.MarshalBinary: len: %w", err)
	}
	n += int(uint32(2 * (datatype.LenInt64 + datatype.LenMeta)))

	valTLV := platformparser.NewTLVUnmarshaler(int64Unmarshaler)
	if err := valTLV.UnmarshalBinary(buf[n:]); err != nil {
		return fmt.Errorf("Item.MarshalBinary: val: %w", err)
	}

	i.val = valTLV.Value
	n += int(valTLV.BytesRead)

	pagePosTLV := platformparser.NewTLVUnmarshaler(int64Unmarshaler)
	if err := pagePosTLV.UnmarshalBinary(buf[n:]); err != nil {
		return fmt.Errorf("Item.MarshalBinary: page pos: %w", err)
	}
	i.PagePos = pagePosTLV.Value
	n += int(pagePosTLV.BytesRead)

	return nil
}

func (i *Index) Add(id, pagePos int64) error {
	itemBuf, err := NewItem(id, pagePos).MarshalBinary()
	if err != nil {
		return fmt.Errorf("index.Add: %w", err)
	}
	int64Marshaler := platformparser.NewValueMarshaler[int64](id)
	idBuf, err := int64Marshaler.MarshalBinary()
	if err != nil {
		return fmt.Errorf("index.Add: %w", err)
	}

	err = i.btree.Put(idBuf, itemBuf)
	if err != nil {
		return fmt.Errorf("index.Add: %w", err)
	}
	return nil
}

func (i *Index) Get(val int64) (Item, error) {
	int64Marshaler := platformparser.NewValueMarshaler[int64](val)
	valBuf, err := int64Marshaler.MarshalBinary()
	if err != nil {
		return Item{}, fmt.Errorf("index.Add: %w", err)
	}

	key, err := i.btree.Get(valBuf)
	if err != nil {
		return Item{}, errors.NewItemNotFoundError(val)
	}
	item := Item{}
	// for now only support return one item
	err = item.UnmarshalBinary(key.V[0])
	if err != nil {
		return Item{}, fmt.Errorf("index.Add: %w", err)
	}

	return item, nil
}
