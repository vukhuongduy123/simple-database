package index

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"simple-database/internal/platform/datatype"
	platformerror "simple-database/internal/platform/error"
	"simple-database/internal/platform/io"
	platformparser "simple-database/internal/platform/parser"
	"simple-database/internal/table/btree"
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

	/*err = binary.Write(&buf, binary.LittleEndian, uint32(len(valBuf)))
	if err != nil {
		return nil, platformerror.NewStackTraceError(err.Error(), platformerror.BinaryWriteErrorCode)
	}*/
	buf.Write(valBuf)

	marshaler = platformparser.NewValueMarshaler[any](k.id)
	idBuf, err := marshaler.MarshalBinaryWithBigEndian()

	/*err = binary.Write(&buf, binary.LittleEndian, uint32(len(idBuf)))
	if err != nil {
		return nil, platformerror.NewStackTraceError(err.Error(), platformerror.BinaryWriteErrorCode)
	}*/
	buf.Write(idBuf)

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
		return platformerror.NewStackTraceError(err.Error(), platformerror.BTreeReadError)
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

func (i *Index) Get(val any, op string) ([]Item, error) {
	// in case of unique index, id is same as val
	var itemKey = NewItemKey(val, val)

	valBuf, err := itemKey.MarshalBinary()
	if err != nil {
		return nil, err
	}

	keys := make([]btree.Key, 0)

	var extractFunc = func(data []byte) []byte {
		if i.unique {
			return data
		}
		// in case of non-unique index, only compare the val part
		// skip data type get the length of val part
		size := binary.LittleEndian.Uint32(data[datatype.LenByte:datatype.LenMeta])
		return data[datatype.LenMeta : datatype.LenMeta+size]
	}

	switch op {
	case datatype.OperatorEqual:
		if i.unique {
			fmt.Println("unique index ", valBuf)
			key, found, err := i.tree.Get(valBuf)
			if err != nil {
				return nil, platformerror.NewStackTraceError(err.Error(), platformerror.BTreeReadError)
			}
			if !found {
				return nil, nil
			}
			keys = append(keys, key)
		} else {
			keys, err = i.tree.GetPrefix(valBuf, extractFunc)
			if err != nil {
				return nil, platformerror.NewStackTraceError(err.Error(), platformerror.BTreeReadError)
			}
		}
	case datatype.OperatorGreater:
		keys, err = i.tree.GreaterThan(valBuf)
		if err != nil {
			return nil, platformerror.NewStackTraceError(err.Error(), platformerror.BTreeReadError)
		}
	case datatype.OperatorLess:
		keys, err = i.tree.LessThan(valBuf)
		if err != nil {
			return nil, platformerror.NewStackTraceError(err.Error(), platformerror.BTreeReadError)
		}
	case datatype.OperatorGreaterOrEqual:
		keys, err = i.tree.GreaterThanOrEqual(valBuf)
		if err != nil {
			return nil, platformerror.NewStackTraceError(err.Error(), platformerror.BTreeReadError)
		}
	case datatype.OperatorLessOrEqual:
		keys, err = i.tree.LessThanOrEqual(valBuf)
		if err != nil {
			return nil, platformerror.NewStackTraceError(err.Error(), platformerror.BTreeReadError)
		}
	case datatype.OperatorNotEqual:
		return nil, platformerror.NewStackTraceError(fmt.Sprintf("Not yet implemented : %v", op), platformerror.UnknownOperatorErrorCode)
	default:
		return nil, platformerror.NewStackTraceError(fmt.Sprintf("Unknown Operator : %v", op), platformerror.UnknownOperatorErrorCode)
	}

	items := make([]Item, 0)

	if keys == nil {
		return nil, nil
	}

	for _, k := range keys {
		item := Item{}
		err := item.UnmarshalBinary(k.V)
		if err != nil {
			return nil, err
		}
		items = append(items, item)
	}

	return items, nil
}

func (i *Index) LogTree() error {
	return i.tree.PrintTree()
}
