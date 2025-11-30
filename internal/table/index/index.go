package index

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"simple-database/internal/platform"
	"simple-database/internal/platform/datatype"
	platformerror "simple-database/internal/platform/error"
	"simple-database/internal/platform/io"
	platformparser "simple-database/internal/platform/parser"
)

type Index struct {
	tree   *platform.BTree
	unique bool
}

type Item struct {
	val     any
	id      any
	PagePos int64
}

func (i *Item) Size() uint32 {
	valSize := 0
	switch i.val.(type) {
	case string:
		valSize = len([]byte(i.val.(string)))
	default:
		valSize = binary.Size(i.val)
	}
	return uint32(datatype.LenInt64+binary.Size(i.id)+valSize) + 2*datatype.LenMeta
}

func NewItem(val, idVal any, pagePos int64) *Item {
	return &Item{
		val:     val,
		id:      idVal,
		PagePos: pagePos,
	}
}

func NewIndex(f string, unique bool) *Index {
	t := platform.Open(f)

	return &Index{tree: t, unique: unique}
}

func (i *Item) MarshalBinary() ([]byte, error) {
	buf := bytes.Buffer{}
	// type
	if err := binary.Write(&buf, binary.LittleEndian, datatype.TypeIndexItem); err != nil {
		return nil, platformerror.NewStackTraceError(err.Error(), platformerror.BinaryWriteErrorCode)
	}
	// len
	if err := binary.Write(&buf, binary.LittleEndian, i.Size()); err != nil {
		return nil, platformerror.NewStackTraceError(err.Error(), platformerror.BinaryWriteErrorCode)
	}

	idValTLV := platformparser.NewTLVMarshaler(i.id)
	idValBuf, err := idValTLV.MarshalBinary()
	if err != nil {
		return nil, err
	}
	buf.Write(idValBuf)

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

	tlvParser := platformparser.NewTLVParser(io.NewReader(bytes.NewReader(buf[n:])))
	idBuf, err := tlvParser.Parse()
	if err != nil {
		return err
	}
	i.id = idBuf
	n += int(tlvParser.BytesRead())

	tlvParser = platformparser.NewTLVParser(io.NewReader(bytes.NewReader(buf[n:])))
	valBuf, err := tlvParser.Parse()
	if err != nil {
		return err
	}
	i.val = valBuf
	n += int(tlvParser.BytesRead())

	pagePosTLV := platformparser.NewTLVUnmarshaler(int64Unmarshaler)
	if err := pagePosTLV.UnmarshalBinary(buf[n:]); err != nil {
		return err
	}
	i.PagePos = pagePosTLV.Value
	n += int(pagePosTLV.BytesRead)

	return nil
}

func (i *Index) Add(val, id any, pagePos int64) error {
	itemBuf, err := NewItem(val, id, pagePos).MarshalBinary()
	if err != nil {
		return err
	}
	marshaler := platformparser.NewValueMarshaler[any](val)
	valBuf, err := marshaler.MarshalBinaryWithBigEndian()
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

	i.tree.Insert(valBuf, itemBuf)
	return nil
}

func (i *Index) Close() error {
	return i.tree.Close()
}

func (i *Index) Remove(key, idVal any) error {
	marshaler := platformparser.NewValueMarshaler[any](key)
	valBuf, err := marshaler.MarshalBinaryWithBigEndian()
	if err != nil {
		return err
	}

	i.tree.Delete(valBuf)

	// TODO: handle if index is a list
	// itemsToRemove := make([][]byte, 0)
	/*if item.id == id {
		buf, err := item.MarshalBinary()
		if err != nil {
			return err
		}
		itemsToRemove = append(itemsToRemove, buf)
	}


	if len(itemsToRemove) == len(key.V) {
		err = i.tree.Delete(valBuf)
		if err != nil {
			return err
		}
	} else {
		for _, v := range itemsToRemove {
			err = i.tree.Remove(valBuf, v)
			if err != nil {
				return err
			}
		}
	}*/

	return nil
}

func (i *Index) Get(val any, op string) ([]Item, error) {
	marshaler := platformparser.NewValueMarshaler[any](val)
	valBuf, err := marshaler.MarshalBinaryWithBigEndian()
	if err != nil {
		return nil, err
	}

	keys := make([]*platform.Item, 0)
	var key *platform.Item

	switch op {
	case datatype.OperatorEqual:
		key = i.tree.Get(valBuf)
		if key == nil {
			return nil, nil
		}
		keys = append(keys, key)
	case datatype.OperatorGreater:
		keys = i.tree.GreaterThan(valBuf)
	case datatype.OperatorLess:
		keys = i.tree.LessThan(valBuf)
	case datatype.OperatorGreaterOrEqual:
		keys = i.tree.GreaterThanOrEqual(valBuf)
	case datatype.OperatorLessOrEqual:
		keys = i.tree.LessThanOrEqual(valBuf)
	case datatype.OperatorNotEqual:
		return nil, errors.ErrUnsupported
	default:
		return nil, platformerror.NewStackTraceError(fmt.Sprintf("Unknown Operator : %v", op), platformerror.UnknownOperatorErrorCode)
	}

	items := make([]Item, 0)

	if keys == nil {
		return nil, nil
	}

	for _, k := range keys {
		item := Item{}
		err = item.UnmarshalBinary(k.Val)
		if err != nil {
			return nil, err
		}

		items = append(items, item)

	}

	return items, nil
}
