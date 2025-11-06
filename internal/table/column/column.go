package column

import (
	"fmt"
	platformerror "simple-database/internal/platform/error"
	"simple-database/internal/table/column/parser"
)

const (
	NameLength byte = 64
)

const (
	Nullable int32 = 1 << iota
	UsingIndex
	UsingUniqueIndex = UsingIndex | 1<<2
)

type Column struct {
	Name         [NameLength]byte
	DataType     byte
	Opts         int32
	IsPrimaryKey bool
}

func (c *Column) Is(flag int32) bool {
	return c.Opts&flag != 0
}

func (c *Column) MarshalBinary() ([]byte, error) {
	return parser.NewColumnDefinitionMarshaler(c.Name, c.DataType, c.IsPrimaryKey, c.Opts).MarshalBinary()
}

func (c *Column) UnmarshalBinary(data []byte) error {
	marshaler := parser.NewColumnDefinitionMarshaler(c.Name, c.DataType, c.IsPrimaryKey, c.Opts)
	if err := marshaler.UnmarshalBinary(data); err != nil {
		return fmt.Errorf("Column.UnmarshalBinary: %w", err)
	}
	c.Name = marshaler.Name
	c.DataType = marshaler.DataType
	c.Opts = marshaler.Opts
	c.IsPrimaryKey = marshaler.IsPrimaryKey
	return nil
}

func NewColumn(name string, dataType byte, IsPrimaryKey bool, opts int32) (*Column, error) {
	if len(name) > int(NameLength) {
		return nil, platformerror.NewStackTraceError(fmt.Sprintf("Expected name length %d, got %d", int(NameLength), len(name)),
			platformerror.InvalidNameLengthErrorCode)
	}
	col := &Column{
		DataType:     dataType,
		IsPrimaryKey: IsPrimaryKey,
		Opts:         opts,
	}
	copy(col.Name[:], name)
	return col, nil
}
