package column

import (
	errors "simple-database/internal/common/error"
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
