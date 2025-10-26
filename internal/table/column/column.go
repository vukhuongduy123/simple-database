package column

import (
	"fmt"
	errors "simple-database/internal/platform/error"
	"simple-database/internal/table/column/parser"
)

const (
	NameLength byte = 64
)

type Column struct {
	Name     [NameLength]byte
	DataType byte
	Opts     Opts
}

type Opts struct {
	AllowNull bool
}

func (c *Column) MarshalBinary() ([]byte, error) {
	return parser.NewColumnDefinitionMarshaler(c.Name, c.DataType, c.Opts.AllowNull).MarshalBinary()
}

func (c *Column) UnmarshalBinary(data []byte) error {
	marshaler := parser.NewColumnDefinitionMarshaler(c.Name, c.DataType, c.Opts.AllowNull)
	if err := marshaler.UnmarshalBinary(data); err != nil {
		return fmt.Errorf("Column.UnmarshalBinary: %w", err)
	}
	c.Name = marshaler.Name
	c.DataType = marshaler.DataType
	c.Opts.AllowNull = marshaler.AllowNull
	return nil
}

func NewOpts(allowNull bool) Opts {
	return Opts{AllowNull: allowNull}
}

func NewColumn(name string, dataType byte, opts Opts) (*Column, error) {
	if len(name) > int(NameLength) {
		return nil, errors.NewNameTooLongError(int(NameLength), len(name))
	}
	col := &Column{
		DataType: dataType,
		Opts:     opts,
	}
	copy(col.Name[:], name)
	return col, nil
}
