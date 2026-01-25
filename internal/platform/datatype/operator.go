package datatype

import (
	"fmt"
	"simple-database/internal/platform/helper"
)

type Operator string

const (
	OperatorEqual          Operator = "Equal"
	OperatorGreater        Operator = "Greater"
	OperatorLess           Operator = "Less"
	OperatorGreaterOrEqual Operator = "GreaterOrEqual"
	OperatorLessOrEqual    Operator = "LessOrEqual"
	OperatorNotEqual       Operator = "NotEqual"
	OperatorAnd            Operator = "And"
	OperatorOr             Operator = "Or"
	OperatorNot            Operator = "Not"
)

var symbolOperatorMap = map[string]Operator{
	"=":   OperatorEqual,
	">":   OperatorGreater,
	"<":   OperatorLess,
	">=":  OperatorGreaterOrEqual,
	"<=":  OperatorLessOrEqual,
	"!=":  OperatorNotEqual,
	"AND": OperatorAnd,
	"OR":  OperatorOr,
	"NOT": OperatorNot,
}

func FromSymbol(symbol string) Operator {
	return symbolOperatorMap[symbol]
}

func Compare(a, b any, op Operator) bool {
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
		panic(fmt.Sprintf("unsupported type: %s", op))
	}
}

func compareScalar[T Scalar](a, b T, op Operator) bool {
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
