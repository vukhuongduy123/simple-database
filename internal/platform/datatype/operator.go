package datatype

import (
	"fmt"
	"simple-database/internal/platform/helper"
)

const (
	OperatorEqual          = "Equal"
	OperatorGreater        = "Greater"
	OperatorLess           = "Less"
	OperatorGreaterOrEqual = "GreaterOrEqual"
	OperatorLessOrEqual    = "LessOrEqual"
	OperatorNotEqual       = "NotEqual"
)

func Compare(a, b any, op string) bool {
	switch va := a.(type) {
	case int:
		vb, ok := b.(int)
		return ok && compareScalar(va, vb, op)
	case int8:
		vb, ok := b.(int8)
		return ok && compareScalar(va, vb, op)
	case int16:
		vb, ok := b.(int16)
		return ok && compareScalar(va, vb, op)
	case int32:
		vb, ok := b.(int32)
		return ok && compareScalar(va, vb, op)
	case int64:
		vb, ok := b.(int64)
		return ok && compareScalar(va, vb, op)
	case uint:
		vb, ok := b.(uint)
		return ok && compareScalar(va, vb, op)
	case uint8:
		vb, ok := b.(uint8)
		return ok && compareScalar(va, vb, op)
	case uint16:
		vb, ok := b.(uint16)
		return ok && compareScalar(va, vb, op)
	case uint32:
		vb, ok := b.(uint32)
		return ok && compareScalar(va, vb, op)
	case uint64:
		vb, ok := b.(uint64)
		return ok && compareScalar(va, vb, op)
	case float32:
		vb, ok := b.(float32)
		return ok && compareScalar(va, vb, op)
	case float64:
		vb, ok := b.(float64)
		return ok && compareScalar(va, vb, op)
	case string:
		vb, ok := b.(string)
		return ok && compareScalar(va, vb, op)
	default:
		panic(fmt.Sprintf("unsupported type: %Degree", a))
	}
}

func compareScalar[T Scalar](a, b T, op string) bool {
	switch op {
	case OperatorEqual:
		if helper.IsFloatingPoint(a) {
			return helper.CompareFloatingPoint(a, b)
		}
		return a == b
	case OperatorNotEqual:
		return a != b
	case OperatorGreater:
		return a > b
	case OperatorGreaterOrEqual:
		return a >= b
	case OperatorLess:
		return a < b
	case OperatorLessOrEqual:
		return a <= b
	default:
		return false
	}
}
