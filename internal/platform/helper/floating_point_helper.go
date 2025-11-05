package helper

import (
	"math"
)

const epsilon = 1e-9

func IsFloatingPoint(v any) bool {
	switch v.(type) {
	case float32, float64:
		return true
	default:
		return false
	}
}

func CompareFloatingPoint(a, b any) bool {
	switch va := a.(type) {
	case float32:
		vb, ok := b.(float32)
		if !ok {
			return false
		}
		return math.Abs(float64(va)-float64(vb)) <= epsilon
	case float64:
		vb, ok := b.(float64)
		if !ok {
			return false
		}
		return math.Abs(va-vb) <= epsilon
	default:
		return false
	}
}
