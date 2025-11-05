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

type Item struct {
	val     any
	PagePos int64
}

func NewItem(val any, pagePos int64) *Item {
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
	n += datatype.LenInt32

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

func (i *Index) Add(val any, pagePos int64) error {
	itemBuf, err := NewItem(val, pagePos).MarshalBinary()
	if err != nil {
		return fmt.Errorf("index.Add: %w", err)
	}
	marshaler := platformparser.NewValueMarshaler[any](val)
	idBuf, err := marshaler.MarshalBinaryWithBigEndian()
	if err != nil {
		return fmt.Errorf("index.Add: %w", err)
	}

	err = i.btree.Delete(idBuf)
	if err != nil {
		return fmt.Errorf("index.Add: %w", err)
	}

	err = i.btree.Put(idBuf, itemBuf)
	if err != nil {
		return fmt.Errorf("index.Add: %w", err)
	}
	return nil
}

func (i *Index) Get(val any) (Item, error) {
	marshaler := platformparser.NewValueMarshaler[any](val)
	valBuf, err := marshaler.MarshalBinaryWithBigEndian()
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

func (i *Index) Close() error {
	return i.btree.Close()
}

func (i *Index) Remove(val any) error {
	marshaler := platformparser.NewValueMarshaler[any](val)
	idBuf, err := marshaler.MarshalBinaryWithBigEndian()
	if err != nil {
		return fmt.Errorf("index.Add: %w", err)
	}
	err = i.btree.Delete(idBuf)
	if err != nil {
		return err
	}
	return nil
}

func (i *Index) RemoveAll(ids []any) error {
	for _, id := range ids {
		err := i.Remove(id)
		if err != nil {
			return err
		}
	}
	return nil
}

// Compare TODO: for now only support first equal item
func (i *Index) Compare(val any, op string) (Item, error) {
	marshaler := platformparser.NewValueMarshaler[any](val)
	valBuf, err := marshaler.MarshalBinaryWithBigEndian()
	if err != nil {
		return Item{}, fmt.Errorf("index.Add: %w", err)
	}

	item := Item{}

	var keys []*btree.Key

	switch op {
	case datatype.OperatorEqual:
		return i.Get(val)
	case datatype.OperatorGreater:
		keys, err = i.btree.GreaterThan(valBuf)
	case datatype.OperatorLess:
		keys, err = i.btree.LessThan(valBuf)
	case datatype.OperatorGreaterOrEqual:
		keys, err = i.btree.GreaterThanEq(valBuf)
	case datatype.OperatorLessOrEqual:
		keys, err = i.btree.LessThanEq(valBuf)
	case datatype.OperatorNotEqual:
		keys = make([]*btree.Key, 0)
		greaterKeys, err := i.btree.GreaterThan(valBuf)
		if err != nil {
			return Item{}, fmt.Errorf("index.Add: %w", err)
		}
		lessKeys, err := i.btree.LessThan(valBuf)
		if err != nil {
			return Item{}, fmt.Errorf("index.Add: %w", err)
		}
		keys = append(keys, greaterKeys...)
		keys = append(keys, lessKeys...)
	default:
		return Item{}, fmt.Errorf("index.Add: Unknown operator: %s", op)
	}

	if err != nil {
		return Item{}, fmt.Errorf("index.Add: %w", err)
	}

	err = item.UnmarshalBinary(keys[0].V[0])
	if err != nil {
		return Item{}, fmt.Errorf("index.Add: %w", err)
	}
	return item, nil
}
