package index

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	btree "simple-database/internal/engine/table/btree"
	"simple-database/internal/platform/datatype"
	platformerror "simple-database/internal/platform/error"
	"simple-database/internal/platform/io"
	platformparser "simple-database/internal/platform/parser"
)

type Index struct {
	tree   *btree.BTree
	unique bool
}

type Item struct {
	itemKey ItemKey
	PagePos int64
}

type ItemKey struct {
	id  any
	val any
}

func NewItemKey(val, idVal any) *ItemKey {
	return &ItemKey{val: val, id: idVal}
}

func (k *ItemKey) MarshalBinary() ([]byte, error) {
	var buf bytes.Buffer

	marshaler := platformparser.NewValueMarshaler[any](k.val)
	valBuf, err := marshaler.MarshalBinaryWithBigEndian()
	if err != nil {
		return nil, err
	}
	buf.Write(valBuf)

	marshaler = platformparser.NewValueMarshaler[any](k.id)
	idBuf, err := marshaler.MarshalBinaryWithBigEndian()
	if err != nil {
		return nil, err
	}
	buf.Write(idBuf)

	return buf.Bytes(), nil
}

func (k *ItemKey) MarshalValueBinary() ([]byte, error) {
	var buf bytes.Buffer

	marshaler := platformparser.NewValueMarshaler[any](k.val)
	valBuf, err := marshaler.MarshalBinaryWithBigEndian()
	if err != nil {
		return nil, err
	}
	buf.Write(valBuf)

	return buf.Bytes(), nil
}

func NewItem(val, idVal any, pagePos int64) *Item {
	return &Item{itemKey: ItemKey{val: val, id: idVal}, PagePos: pagePos}
}

func NewIndex(f string, unique bool) *Index {
	t, err := btree.Open(f)
	if err != nil {
		panic(err)
	}

	return &Index{tree: t, unique: unique}
}

func (i *Item) MarshalBinary() ([]byte, error) {
	buf := bytes.Buffer{}
	// type
	if err := binary.Write(&buf, binary.LittleEndian, datatype.TypeIndexItem); err != nil {
		return nil, platformerror.NewStackTraceError(err.Error(), platformerror.BinaryWriteErrorCode)
	}

	itemKeyBuf, err := i.itemKey.MarshalBinary()

	size := datatype.LenMeta + datatype.LenInt32 + len(itemKeyBuf)
	// len
	if err := binary.Write(&buf, binary.LittleEndian, int32(size)); err != nil {
		return nil, platformerror.NewStackTraceError(err.Error(), platformerror.BinaryWriteErrorCode)
	}

	idValTLV := platformparser.NewTLVMarshaler(i.itemKey.id)
	idValBuf, err := idValTLV.MarshalBinary()
	if err != nil {
		return nil, err
	}
	buf.Write(idValBuf)

	valTLV := platformparser.NewTLVMarshaler(i.itemKey.val)
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

	tlvParser := platformparser.NewTLVParser(io.NewReader(bytes.NewReader(buf[n:])))
	idBuf, err := tlvParser.Parse()
	if err != nil {
		return err
	}
	i.itemKey.id = idBuf
	n += int(tlvParser.BytesRead())

	tlvParser = platformparser.NewTLVParser(io.NewReader(bytes.NewReader(buf[n:])))
	valBuf, err := tlvParser.Parse()
	if err != nil {
		return err
	}
	i.itemKey.id = valBuf
	n += int(tlvParser.BytesRead())

	pagePosTLV := platformparser.NewTLVUnmarshaler(int64Unmarshaler)
	if err := pagePosTLV.UnmarshalBinary(buf[n:]); err != nil {
		return err
	}
	i.PagePos = pagePosTLV.Value
	n += int(pagePosTLV.BytesRead)

	return nil
}

func (i *Index) Add(item *Item) error {
	itemBuf, err := item.MarshalBinary()
	if err != nil {
		return err
	}

	if i.unique {
		items, err := i.Get(item.itemKey.val, datatype.OperatorEqual)
		if err != nil {
			return err
		}

		if items != nil {
			return platformerror.NewStackTraceError(fmt.Sprintf("Unique key validate with value: %v", item.itemKey.val), platformerror.UniqueKeyViolationErrorCode)
		}
	}

	keyBuf, err := item.itemKey.MarshalBinary()
	if err != nil {
		return err
	}

	err = i.tree.Insert(keyBuf, itemBuf)
	if err != nil {
		return platformerror.NewStackTraceError(err.Error(), platformerror.BTreeReadErrorCode)
	}
	return nil
}

func (i *Index) Close() error {
	return i.tree.Close()
}

func (i *Index) Remove(key *ItemKey) error {
	idBuf, err := key.MarshalBinary()
	if err != nil {
		return err
	}

	err = i.tree.Remove(idBuf)
	if err != nil {
		return err
	}
	return nil
}

func appendSlice(source []byte, b byte) []byte {
	size := int(math.Ceil(btree.PageSize/btree.DefaultDegree) * 1.5)

	dest := make([]byte, size)
	copy(dest, source)

	for i := len(source); i < size; i++ {
		dest[i] = b
	}
	return dest
}

//goland:noinspection DuplicatedCode
func (i *Index) Get(val any, op datatype.Operator) ([]Item, error) {
	// in case of unique index, id is same as val
	var itemKey = NewItemKey(val, val)

	itemKeyBuf, err := itemKey.MarshalBinary()
	if err != nil {
		return nil, err
	}

	keys := make([]btree.Key, 0)

	switch op {
	case datatype.OperatorEqual:
		if i.unique {
			key, found, err := i.tree.Get(itemKeyBuf)
			if err != nil {
				return nil, platformerror.NewStackTraceError(err.Error(), platformerror.BTreeReadErrorCode)
			}
			if !found {
				return nil, nil
			}
			keys = append(keys, key)
		} else {
			keyBuf, err := itemKey.MarshalValueBinary()
			if err != nil {
				return nil, err
			}

			// TODO: edge case where prefix search can return wrong results if val + part of id as byte same as another val
			keys, err = i.tree.GetPrefix(keyBuf)
			if err != nil {
				return nil, platformerror.NewStackTraceError(err.Error(), platformerror.BTreeReadErrorCode)
			}
		}
	case datatype.OperatorGreater:
		if i.unique {
			keys, err = i.tree.GreaterThan(itemKeyBuf)
			if err != nil {
				return nil, platformerror.NewStackTraceError(err.Error(), platformerror.BTreeReadErrorCode)
			}
		} else {
			keyBuf, err := itemKey.MarshalValueBinary()
			if err != nil {
				return nil, err
			}
			keys, err = i.tree.GreaterThan(keyBuf)
			if err != nil {
				return nil, platformerror.NewStackTraceError(err.Error(), platformerror.BTreeReadErrorCode)
			}
		}
	case datatype.OperatorLess:
		if i.unique {
			keys, err = i.tree.LessThan(itemKeyBuf)
			if err != nil {
				return nil, platformerror.NewStackTraceError(err.Error(), platformerror.BTreeReadErrorCode)
			}
		} else {
			keyBuf, err := itemKey.MarshalValueBinary()
			if err != nil {
				return nil, err
			}
			keys, err = i.tree.LessThan(keyBuf)
			if err != nil {
				return nil, platformerror.NewStackTraceError(err.Error(), platformerror.BTreeReadErrorCode)
			}
		}
	case datatype.OperatorGreaterOrEqual:
		if i.unique {
			keys, err = i.tree.GreaterThanOrEqual(itemKeyBuf)
			if err != nil {
				return nil, platformerror.NewStackTraceError(err.Error(), platformerror.BTreeReadErrorCode)
			}
		} else {
			keyBuf, err := itemKey.MarshalValueBinary()
			if err != nil {
				return nil, err
			}
			keys, err = i.tree.GreaterThanOrEqual(appendSlice(keyBuf, byte(0x00)))
			if err != nil {
				return nil, platformerror.NewStackTraceError(err.Error(), platformerror.BTreeReadErrorCode)
			}
		}

	case datatype.OperatorLessOrEqual:
		if i.unique {
			keys, err = i.tree.LessThanOrEqual(itemKeyBuf)
			if err != nil {
				return nil, platformerror.NewStackTraceError(err.Error(), platformerror.BTreeReadErrorCode)
			}
		} else {
			keyBuf, err := itemKey.MarshalValueBinary()
			if err != nil {
				return nil, err
			}
			keys, err = i.tree.LessThanOrEqual(appendSlice(keyBuf, byte(0xff)))
			if err != nil {
				return nil, platformerror.NewStackTraceError(err.Error(), platformerror.BTreeReadErrorCode)
			}
		}
	case datatype.OperatorNotEqual:
		return nil, platformerror.NewStackTraceError(fmt.Sprintf("Not yet implemented : %v", op), platformerror.UnknownOperatorErrorCode)
	default:
		return nil, platformerror.NewStackTraceError(fmt.Sprintf("Unknown Operator : %v", op), platformerror.UnknownOperatorErrorCode)
	}

	items := make(map[int64]Item)

	if keys == nil {
		return nil, nil
	}

	for _, k := range keys {
		item := Item{}
		err := item.UnmarshalBinary(k.V)
		if err != nil {
			return nil, err
		}
		if _, exists := items[item.PagePos]; !exists {
			items[item.PagePos] = item
		}
	}
	uniqueItems := make([]Item, 0)
	for _, v := range items {
		uniqueItems = append(uniqueItems, v)
	}

	return uniqueItems, nil
}

func (i *Index) LogTree() error {
	return i.tree.PrintTree()
}

func (i *Index) Drop() {

}
