package index

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"simple-database/internal/platform/datatype"
	errors "simple-database/internal/platform/error"
	platformparser "simple-database/internal/platform/parser"

	"github.com/google/btree"
)

type Index struct {
	btree *btree.BTreeG[Item]
	file  *os.File
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

func NewIndex(f *os.File) *Index {
	bt := btree.NewG[Item](2, func(a, b Item) bool {
		return a.val < b.val
	})
	return &Index{
		btree: bt,
		file:  f,
	}
}

func (i *Index) MarshalBinary() ([]byte, error) {
	buf := bytes.Buffer{}

	// type
	if err := binary.Write(&buf, binary.LittleEndian, datatype.TypeIndex); err != nil {
		return nil, fmt.Errorf("index.MarshalBinary: type: %w", err)
	}

	// length
	item := Item{}
	itemsLen := uint32(i.btree.Len()) * (item.TLVLength() + datatype.LenMeta)
	if err := binary.Write(&buf, binary.LittleEndian, itemsLen); err != nil {
		return nil, fmt.Errorf("index.MarshalBinary: len: %w", err)
	}

	for _, v := range i.GetAll() {
		data, err := v.MarshalBinary()
		if err != nil {
			return nil, fmt.Errorf("index.MarshalBinary: value: %w", err)
		}
		buf.Write(data)
	}
	return buf.Bytes(), nil
}

func (i *Index) GetAll() []Item {
	out := make([]Item, 0)
	i.btree.Ascend(func(a Item) bool {
		out = append(out, a)
		return true
	})
	return out
}

func (i *Item) TLVLength() uint32 {
	return uint32(2*datatype.LenInt64 + 2*datatype.LenMeta)
}

func (i *Item) MarshalBinary() ([]byte, error) {
	buf := bytes.Buffer{}
	// type
	if err := binary.Write(&buf, binary.LittleEndian, datatype.TypeIndexItem); err != nil {
		return nil, fmt.Errorf("Item.MarshalBinary: type: %w", err)
	}
	// len
	if err := binary.Write(&buf, binary.LittleEndian, i.TLVLength()); err != nil {
		return nil, fmt.Errorf("Item.MarshalBinary: len: %w", err)
	}
	idTLV := platformparser.NewTLVMarshaler(i.val)
	idBuf, err := idTLV.MarshalBinary()
	if err != nil {
		return nil, fmt.Errorf("Item.MarshalBinary: ID TLV: %w", err)
	}
	buf.Write(idBuf)

	pagePosTLV := platformparser.NewTLVMarshaler(i.PagePos)
	pagePosBuf, err := pagePosTLV.MarshalBinary()
	if err != nil {
		return nil, fmt.Errorf("Item.MarshalBinary: page pos: %w", err)
	}
	buf.Write(pagePosBuf)
	return buf.Bytes(), nil

}

func (i *Index) persist() error {
	if err := i.file.Truncate(0); err != nil {
		return fmt.Errorf("index.persist: file.Truncate: %w", err)
	}
	if _, err := i.file.Seek(0, io.SeekStart); err != nil {
		return fmt.Errorf("index.persist: file.Seek: %w", err)
	}
	b, err := i.MarshalBinary()
	if err != nil {
		return fmt.Errorf("index.persist: marshalBinary: %w", err)
	}
	n, err := i.file.Write(b)
	if err != nil {
		return fmt.Errorf("index.persist: file.Write: %w", err)
	}
	if n != len(b) {
		return errors.NewIncompleteWriteError(len(b), n)
	}
	return nil
}

func (i *Index) AddAndPersist(id, pagePos int64) error {
	i.btree.ReplaceOrInsert(*NewItem(id, pagePos))
	return i.persist()
}

func (i *Index) UnmarshalBinary(data []byte) error {
	byteUnmarshaler := platformparser.NewValueUnmarshaler[byte]()
	int32Unmarshaler := platformparser.NewValueUnmarshaler[uint32]()
	int64Unmarshaler := platformparser.NewValueUnmarshaler[int64]()

	n := 0
	// type
	if err := byteUnmarshaler.UnmarshalBinary(data); err != nil {
		return fmt.Errorf("index.UnmarshalBinary: type: %w", err)
	}
	n++
	// len
	if err := int32Unmarshaler.UnmarshalBinary(data[n:]); err != nil {
		return fmt.Errorf("index.UnmarshalBinary: len: %w", err)
	}
	n += datatype.LenInt32

	for {
		// type of index item
		if err := byteUnmarshaler.UnmarshalBinary(data[n:]); err != nil {
			if err == io.EOF {
				return nil
			}
			return fmt.Errorf("index.UnmarshalBinary: ID type: %w", err)
		}
		n++
		// len of index item
		if err := int32Unmarshaler.UnmarshalBinary(data[n:]); err != nil {
			return fmt.Errorf("index.UnmarshalBinary: ID len: %w", err)
		}
		n += datatype.LenInt32

		idTLV := platformparser.NewTLVUnmarshaler(int64Unmarshaler)
		if err := idTLV.UnmarshalBinary(data[n:]); err != nil {
			return fmt.Errorf("index.UnmarshalBinary: ID TLV: %w", err)
		}
		n += int(idTLV.BytesRead)
		id := idTLV.Value

		pagePosTLV := platformparser.NewTLVUnmarshaler(int64Unmarshaler)
		if err := pagePosTLV.UnmarshalBinary(data[n:]); err != nil {
			return fmt.Errorf("index.UnmarshalBinary: page pos: %w", err)
		}
		n += int(pagePosTLV.BytesRead)
		pagePos := pagePosTLV.Value
		i.Add(id, pagePos)
	}
}
func (i *Index) Add(id, pagePos int64) {
	i.btree.ReplaceOrInsert(*NewItem(id, pagePos))
}

func (i *Index) Load() error {
	if _, err := i.file.Seek(0, io.SeekStart); err != nil {
		return fmt.Errorf("index.Load: file.Seek: %w", err)
	}
	stat, err := i.file.Stat()
	if err != nil {
		return fmt.Errorf("index.Load: file.Stat: %w", err)
	}
	b := make([]byte, stat.Size())
	n, err := i.file.Read(b)
	if err != nil {
		return fmt.Errorf("index.Load: file.Read: %w", err)
	}
	if n != len(b) {
		return errors.NewIncompleteReadError(len(b), n)
	}
	if err = i.UnmarshalBinary(b); err != nil {
		return fmt.Errorf("index.Load: UnmarshalBinary: %w", err)
	}
	return nil
}

func (i *Index) Get(val int64) (Item, error) {
	item, ok := i.btree.Get(Item{val: val})
	if !ok {
		return Item{}, errors.NewItemNotFoundError(val)
	}
	return item, nil
}
