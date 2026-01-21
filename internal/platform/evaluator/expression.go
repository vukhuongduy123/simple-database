package evaluator

import (
	"simple-database/internal/platform/datatype"
)

type Expression struct {
	Left  any
	Op    string
	Right any
}

func (e *Expression) Keys() []string {
	keys := make(map[string]struct{})
	e.collectKeys(keys)

	result := make([]string, 0, len(keys))
	for k := range keys {
		result = append(result, k)
	}
	return result
}

func (e *Expression) ValueAndOperator(key string) (any, string) {
	// Check current node
	if leftKey, ok := e.Left.(string); ok && leftKey == key {
		return e.Right, e.Op
	}

	// Recurse into the Left subtree
	switch l := e.Left.(type) {
	case Expression:
		if val, op := l.ValueAndOperator(key); op != "" {
			return val, op
		}
	case *Expression:
		if l != nil {
			if val, op := l.ValueAndOperator(key); op != "" {
				return val, op
			}
		}
	}

	// Recurse into the Right subtree
	switch r := e.Right.(type) {
	case Expression:
		if val, op := r.ValueAndOperator(key); op != "" {
			return val, op
		}
	case *Expression:
		if r != nil {
			if val, op := r.ValueAndOperator(key); op != "" {
				return val, op
			}
		}
	}

	return "", ""
}

func (e *Expression) collectKeys(out map[string]struct{}) {
	if e == nil {
		return
	}

	// check left
	switch v := e.Left.(type) {
	case string:
		out[v] = struct{}{}
	case Expression:
		v.collectKeys(out)
	case *Expression:
		v.collectKeys(out)
	}

	// check right
	switch v := e.Right.(type) {
	case string:
		out[v] = struct{}{}
	case Expression:
		v.collectKeys(out)
	case *Expression:
		v.collectKeys(out)
	}
}

type Evaluator interface {
	Eval(expr Expression, row map[string]any) bool
}

type SimpleEvaluator struct{}

func (e *SimpleEvaluator) Eval(expr Expression, row map[string]any) bool {
	left := e.evalValue(expr.Left, row)
	right := e.evalValue(expr.Right, row)

	switch expr.Op {
	case datatype.OperatorAnd:
		return left.(bool) && right.(bool)
	case datatype.OperatorOr:
		return left.(bool) || right.(bool)
	case datatype.OperatorNotEqual:
		return left.(bool) != right.(bool)
	case datatype.OperatorNot:
		return !left.(bool)
	default:
		return datatype.Compare(left, right, expr.Op)
	}
}

func (e *SimpleEvaluator) evalValue(v any, row map[string]any) any {
	switch x := v.(type) {
	case Expression:
		return e.Eval(x, row)
	case *Expression:
		return e.Eval(*x, row)
	case string:
		// treat as column name ONLY if present in a row
		if val, ok := row[x]; ok {
			return val
		}
		// otherwise literal string
		return x
	default:
		return x
	}
}
