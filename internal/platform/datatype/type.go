package datatype

//goland:noinspection GoUnusedConst
const (
	TypeInt64            byte = 10
	TypeString           byte = 2
	TypeByte             byte = 3
	TypeBool             byte = 4
	TypeInt32            byte = 5
	TypeByteArray        byte = 6
	TypeColumnDefinition byte = 99
	TypeRecord           byte = 100
	TypeDeletedRecord    byte = 101
	TypeBTreeKeyValue    byte = 102
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

func IsScalar(v any) bool {
	switch v.(type) {
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64, float32, float64, string:
		return true
	default:
		return false
	}
}
