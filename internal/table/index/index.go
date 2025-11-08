package index

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"os"
	"simple-database/internal/platform/datatype"
	platformerror "simple-database/internal/platform/error"
	platformparser "simple-database/internal/platform/parser"

	"github.com/guycipher/btree"
)

type Index struct {
	btree  *btree.BTree
	unique bool
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

func NewIndex(f string, unique bool) *Index {
	bt, _ := btree.Open(f, os.O_CREATE|os.O_RDWR, 0644, 3)

	return &Index{btree: bt, unique: unique}
}

func (i *Item) MarshalBinary() ([]byte, error) {
	buf := bytes.Buffer{}
	// type
	if err := binary.Write(&buf, binary.LittleEndian, datatype.TypeIndexItem); err != nil {
		return nil, platformerror.NewStackTraceError(err.Error(), platformerror.BinaryWriteErrorCode)
	}
	// len
	if err := binary.Write(&buf, binary.LittleEndian, uint32(2*(datatype.LenInt64+datatype.LenMeta))); err != nil {
		return nil, platformerror.NewStackTraceError(err.Error(), platformerror.BinaryWriteErrorCode)
	}

	valTLV := platformparser.NewTLVMarshaler(i.val)
	valBuf, err := valTLV.MarshalBinary()
	if err != nil {
		return nil, err
	}
	buf.Write(valBuf)

	pagePosTLV := platformparser.NewTLVMarshaler(i.PagePos)
	pagePosBuf, err := pagePosTLV.MarshalBinary()
	if err != nil {
		return nil, err
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
		return err
	}
	n += datatype.LenByte

	// len
	if err := int32Unmarshaler.UnmarshalBinary(buf[n:]); err != nil {
		return err
	}
	n += datatype.LenInt32

	valTLV := platformparser.NewTLVUnmarshaler(int64Unmarshaler)
	if err := valTLV.UnmarshalBinary(buf[n:]); err != nil {
		return err
	}

	i.val = valTLV.Value
	n += int(valTLV.BytesRead)

	pagePosTLV := platformparser.NewTLVUnmarshaler(int64Unmarshaler)
	if err := pagePosTLV.UnmarshalBinary(buf[n:]); err != nil {
		return err
	}
	i.PagePos = pagePosTLV.Value
	n += int(pagePosTLV.BytesRead)

	return nil
}

func (i *Index) Add(val any, pagePos int64) error {
	itemBuf, err := NewItem(val, pagePos).MarshalBinary()
	if err != nil {
		return err
	}
	marshaler := platformparser.NewValueMarshaler[any](val)
	idBuf, err := marshaler.MarshalBinaryWithBigEndian()
	if err != nil {
		return err
	}

	if i.unique {
		items, err := i.Get(val, datatype.OperatorEqual)
		if err != nil {
			return err
		}

		if items != nil {
			return platformerror.NewStackTraceError(fmt.Sprintf("Unique key validate with value: %v", val), platformerror.UniqueKeyViolationErrorCode)
		}
	}

	err = i.btree.Put(idBuf, itemBuf)
	if err != nil {
		return platformerror.NewStackTraceError(err.Error(), platformerror.BTreeErrorCode)
	}
	return nil
}

func (i *Index) Close() error {
	return i.btree.Close()
}

func (i *Index) Remove(val any) error {
	marshaler := platformparser.NewValueMarshaler[any](val)
	idBuf, err := marshaler.MarshalBinaryWithBigEndian()
	if err != nil {
		return err
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

func (i *Index) Get(val any, op string) ([]Item, error) {
	marshaler := platformparser.NewValueMarshaler[any](val)
	valBuf, err := marshaler.MarshalBinaryWithBigEndian()
	if err != nil {
		return nil, err
	}

	var keys []*btree.Key

	switch op {
	case datatype.OperatorEqual:
		key, e := i.btree.Get(valBuf)
		if key == nil {
			return nil, nil
		}
		err = e
		keys = append(keys, key)
	case datatype.OperatorGreater:
		keys, err = i.btree.GreaterThan(valBuf)
	case datatype.OperatorLess:
		keys, err = i.btree.LessThan(valBuf)
	case datatype.OperatorGreaterOrEqual:
		keys, err = i.btree.GreaterThanEq(valBuf)
	case datatype.OperatorLessOrEqual:
		keys, err = i.btree.LessThanEq(valBuf)
	case datatype.OperatorNotEqual:
		keys, err = i.btree.NGet(valBuf)
	default:
		return nil, platformerror.NewStackTraceError(fmt.Sprintf("Unknown Operator : %v", op), platformerror.UnknownOperatorErrorCode)
	}

	if err != nil {
		return nil, platformerror.NewStackTraceError(err.Error(), platformerror.BTreeErrorCode)
	}

	items := make([]Item, 0)

	if keys == nil {
		return nil, nil
	}

	for _, key := range keys {
		for _, v := range key.V {
			item := Item{}
			err = item.UnmarshalBinary(v)
			if err != nil {
				return nil, err
			}

			items = append(items, item)
		}
	}

	return items, nil
}
