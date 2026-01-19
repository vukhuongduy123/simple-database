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
	Normal           = 0
	UsingIndex       = 1 << 0
	UsingUniqueIndex = UsingIndex | 1<<(1)
	PrimaryKey       = UsingUniqueIndex | 1<<(2)
)

type Column struct {
	Name     [NameLength]byte
	DataType byte
	Opts     int32
}

func (c *Column) Is(flag int32) bool {
	return c.Opts&flag == flag
}

func (c *Column) MarshalBinary() ([]byte, error) {
	return parser.NewColumnDefinitionMarshaler(c.Name, c.DataType, c.Opts).MarshalBinary()
}

func (c *Column) UnmarshalBinary(data []byte) error {
	marshaler := parser.NewColumnDefinitionMarshaler(c.Name, c.DataType, c.Opts)
	if err := marshaler.UnmarshalBinary(data); err != nil {
		return err
	}
	c.Name = marshaler.Name
	c.DataType = marshaler.DataType
	c.Opts = marshaler.Opts
	return nil
}

func NewColumn(name string, dataType byte, opts int32) (*Column, error) {
	if len(name) > int(NameLength) {
		return nil, platformerror.NewStackTraceError(fmt.Sprintf("Expected name length %d, got %d", int(NameLength), len(name)),
			platformerror.InvalidNameLengthErrorCode)
	}
	col := &Column{
		DataType: dataType,
		Opts:     opts,
	}
	copy(col.Name[:], name)
	return col, nil
}
