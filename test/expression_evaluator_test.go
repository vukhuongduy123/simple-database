package test

import (
	"simple-database/internal/platform/datatype"
	"simple-database/internal/platform/evaluator"
	"testing"
)

func TestEvaluator(t *testing.T) {
	e := &evaluator.SimpleEvaluator{}

	tests := []struct {
		name string
		expr evaluator.Expression
		want bool
	}{
		{
			name: "equals true",
			expr: evaluator.Expression{Left: "a", Op: datatype.OperatorEqual, Right: 3},
			want: true,
		},
		{
			name: "equals false",
			expr: evaluator.Expression{Left: "c", Op: datatype.OperatorEqual, Right: 4},
			want: false,
		},
		{
			name: "greater than",
			expr: evaluator.Expression{Left: "b", Op: datatype.OperatorGreater, Right: 2},
			want: true,
		},
		{
			name: "less than",
			expr: evaluator.Expression{Left: "d", Op: datatype.OperatorLess, Right: 2},
			want: true,
		},
	}

	row := map[string]any{}
	row["a"] = 3
	row["b"] = 5
	row["c"] = 5
	row["d"] = 1

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := e.Eval(tt.expr, row)
			if got != tt.want {
				t.Fatalf("got %v, want %v", got, tt.want)
			}
		})
	}
}

func TestExpression_Nested(t *testing.T) {
	e := &evaluator.SimpleEvaluator{}

	expr := evaluator.Expression{
		Left: evaluator.Expression{
			Left:  "a",
			Op:    datatype.OperatorGreater,
			Right: 3,
		},
		Op: datatype.OperatorAnd,
		Right: evaluator.Expression{
			Left:  "b",
			Op:    datatype.OperatorLess,
			Right: 4,
		},
	}

	row := map[string]any{}
	row["a"] = 4
	row["b"] = 2

	if !e.Eval(expr, row) {
		t.Fatalf("expected expression to evaluate to true")
	}
}

func TestExpression_DeepNested(t *testing.T) {
	e := &evaluator.SimpleEvaluator{}

	expr := evaluator.Expression{
		// LEFT side of OR
		Left: evaluator.Expression{
			Left: evaluator.Expression{
				Left:  "a",
				Op:    datatype.OperatorGreater,
				Right: 5,
			},
			Op: datatype.OperatorAnd,
			Right: evaluator.Expression{
				Left:  "b",
				Op:    datatype.OperatorLess,
				Right: 8,
			},
		},

		Op: datatype.OperatorOr,

		// RIGHT side of OR
		Right: evaluator.Expression{
			Left: evaluator.Expression{
				Left:  "c",
				Op:    datatype.OperatorEqual,
				Right: 7,
			},
			Op: datatype.OperatorAnd,
			Right: evaluator.Expression{
				// NOT (m >= n)
				Left: evaluator.Expression{
					Left:  "d",
					Op:    datatype.OperatorGreaterOrEqual,
					Right: 6,
				},
				Op:    datatype.OperatorNot,
				Right: nil,
			},
		},
	}

	row := map[string]any{}
	row["a"] = 10
	row["b"] = 3
	row["c"] = 7
	row["d"] = 4

	if !e.Eval(expr, row) {
		t.Fatalf("expected complex expression to evaluate to true")
	}
}
