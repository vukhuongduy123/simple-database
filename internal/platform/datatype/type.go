package datatype

const (
	TypeInt64            byte = 1
	TypeString           byte = 2
	TypeByte             byte = 3
	TypeBool             byte = 4
	TypeInt32            byte = 5
	TypeColumnDefinition byte = 99
	TypeRecord           byte = 100
	TypeDeletedRecord    byte = 101
	TypeWALEntry         byte = 20
	TypePage             byte = 255
	TypeIndex            byte = 254
	TypeIndexItem        byte = 253
)

const (
	LenByte  = 1
	LenInt32 = 4
	LenInt64 = 8
	LenMeta  = 5
)

type Scalar interface {
	~int | ~int8 | ~int16 | ~int32 | ~int64 |
		~uint | ~uint8 | ~uint16 | ~uint32 | ~uint64 |
		~float32 | ~float64 | ~string
}
