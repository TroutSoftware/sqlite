// Virtual Table implementation

package sqlite

import (
	"context"
	"fmt"
	"hash/maphash"
	"strconv"
)

// A constraint is a filter (technically can also be a join, …) passed by the engine to the API implementation.
// The Value is only set at filter stage, not when the plan is created.
// Following SQLite weakly typed system, it is not possible to know the Value without checking its type explicitly, or instead using the accessor functions.
type Constraint struct {
	Column    string
	Operation Op
	Value     any
	num       int // index in table schema
}

// Constraints is a type wrapper around an array of constraints.
type Constraints []Constraint

func (v Constraint) ValueString() string { return fmt.Sprint(v.Value) }
func (v Constraint) ValueBytes() []byte {
	switch vv := v.Value.(type) {
	default:
		return nil
	case []byte:
		return vv
	case string:
		return []byte(vv)
	}
}
func (v Constraint) ValueBool() bool {
	switch vv := v.Value.(type) {
	default:
		return false
	case string:
		return vv == "true"
	case int:
		return vv == 1
	}
}
func (v Constraint) ValueInt() int {
	switch vv := v.Value.(type) {
	default:
		return 0
	case int:
		return vv
	}
}

// note: when adding a new accessor, also update constrainer’s main.go

// Op are the type of filtering operations that the engine can ask
type Op uint8

func (op Op) String() string {
	switch op {
	case ConstraintEQ:
		return "="
	case ConstraintGT:
		return ">"
	case ConstraintLE:
		return "<="
	case ConstraintLT:
		return "<"
	case ConstraintGE:
		return ">="
	case ConstraintMATCH:
		return "match"
	case ConstraintLIKE:
		return "like"
	case ConstraintGLOB:
		return "glob"
	case ConstraintREGEXP:
		return "regexp"
	case ConstraintNE:
		return "!="
	case ConstraintISNOT:
		return "isnot"
	case ConstraintISNOTNULL:
		return "isnotnull"
	case ConstraintISNULL:
		return "isnull"
	case ConstraintIS:
		return "is"
	case ConstraintLIMIT:
		return "limit"
	case ConstraintOFFSET:
		return "offset"
	default:
		return strconv.FormatInt(int64(op), 10)
	}
}

func (op *Op) Scan(st fmt.ScanState, verb rune) error {
	tk, err := st.Token(true, nil)
	if err != nil {
		return err
	}

	switch string(tk) {
	case "=":
		*op = ConstraintEQ
	case ">":
		*op = ConstraintGT
	case "<=":
		*op = ConstraintLE
	case "<":
		*op = ConstraintLT
	case ">=":
		*op = ConstraintGE
	case "match":
		*op = ConstraintMATCH
	case "like":
		*op = ConstraintLIKE
	case "glob":
		*op = ConstraintGLOB
	case "regexp":
		*op = ConstraintREGEXP
	case "!=":
		*op = ConstraintNE
	case "isnot":
		*op = ConstraintISNOT
	case "isnotnull":
		*op = ConstraintISNOTNULL
	case "isnull":
		*op = ConstraintISNULL
	case "is":
		*op = ConstraintIS
	case "limit":
		*op = ConstraintLIMIT
	case "offset":
		*op = ConstraintOFFSET
	default:
		idx, err := strconv.ParseUint(string(tk), 10, 8)
		if err != nil {
			return fmt.Errorf("unknown operator %s", tk)
		}
		*op = Op(idx)
	}
	return nil
}

const (
	ConstraintEQ        Op = 2
	ConstraintGT        Op = 4
	ConstraintLE        Op = 8
	ConstraintLT        Op = 16
	ConstraintGE        Op = 32
	ConstraintMATCH     Op = 64
	ConstraintLIKE      Op = 65
	ConstraintGLOB      Op = 66
	ConstraintREGEXP    Op = 67
	ConstraintNE        Op = 68
	ConstraintISNOT     Op = 69
	ConstraintISNOTNULL Op = 70
	ConstraintISNULL    Op = 71
	ConstraintIS        Op = 72
	ConstraintLIMIT     Op = 73
	ConstraintOFFSET    Op = 74
	ConstraintFUNCTION  Op = 150
)

// A resource represents an abstraction over a signal that can be represented as a virtual table.
//
// The T type is bound in the [RegisterTable] method to the values returned in the iterator.
// We use tags to associate values automatically with virtual table columns:
//
//	// Field will be bound as column “myName”
//	Field int `vtab:"myName"`
//
//	// Field will be bound as “myName” and
//	// the query will fail if “myName” is not specified in the WHERE clause
//	Field int `vtab:"myName,required"
//
//	// Field is ignored by this package
//	Field int `vtab:"-"
//
// The columns values allocated by standard SQLite [affinity].
//
// # Query plan
//
// Resources interact with query plan generation through a simple algorithm:
// each column filtered reduces by a constant factor the cost of the query
// (thus having multiple columns filtered increases the chance of the plan being selected).
// Filtered, required and hidden field are considered equal for the query plan.
// Furthermore, only [ConstraintEQ] match are accepted.
//
// For more control over the plan, implement directly [Indexer] instead.
//
// # Concurrency
//
// [Filtrer.Filter] is called against the global variable used in registering the virtual table.
// The global variable can be used to set constant values, including channels if needed.
//
// Mutation in the global variable should be avoided in the general case;
// the implementation does not try to limit or otherwise protect concurrent access.
//
// Avoid blocking in the [Filtrer.Filter] method, as this would impact all virtual tables.
// Instead, if a form of rate limit is required, prefer returning a well-known error.
//
// [affinity]: https://www.sqlite.org/datatype3.html
type Filtrer[T any] interface {
	Filter(int, Constraints) Iter[T]
}

// Iter represents an iterator over multiple results of an API.
// Implementations must be safe for concurrent use.
// The iterator is going to be primed by calling [Iter.Next] before any value is requested.
type Iter[T any] interface {
	Next() bool
	Err() error
	Value() T
}

// Hasher can be implemented by resources that exhibit referential transparency.
//
// Databases use the idea of a primary key (or row ID), a unique identifier for the relation.
// When the relation is dynamic, it is not generally possible to express this identity;
// and by default, all relations in a virtual table are considered to have a different primary key.
// Implementations that want to overwrite this behavior should implement this interface.
type Hasher interface {
	Hash64() int64
}

// HashSeed is a convenience seed to use when implementing the [Hasher] interface.
var HashSeed = maphash.MakeSeed()

// HashString returns a unique hash, valid as an index
func HashString(s string) int64 { return int64(maphash.String(HashSeed, s) >> 1) }

type iterArray[T any] struct {
	table []T
	seen  int
}

func FromArray[T any](values []T) Iter[T] { return &iterArray[T]{table: values} }
func FromError[T any](err error) Iter[T]  { return &iterError[T]{err: err} }
func FromOne[T any](v T) Iter[T]          { return &iterone[T]{v: v} }
func None[T any]() Iter[T]                { return &iterNone[T]{} }
func TransformArray[T, U any](values []T, trans func(T) U) Iter[U] {
	return &iterTrans[T, U]{base: FromArray(values), trans: trans}
}

func (it *iterArray[T]) Next() bool {
	if len(it.table) <= it.seen {
		return false
	}
	it.seen++
	return true
}

func (it *iterArray[T]) Err() error { return nil }
func (it *iterArray[T]) Value() T   { return it.table[it.seen-1] }

type iterone[T any] struct {
	v     T
	later bool
}

func (r iterone[T]) Err() error { return nil }
func (r *iterone[T]) Next() bool {
	if r.later {
		return false
	}
	r.later = true
	return true
}
func (r iterone[T]) Value() T { return r.v }

type iterError[T any] struct{ err error }

func (it *iterError[T]) Next() bool { return false }
func (it *iterError[T]) Err() error { return it.err }
func (it *iterError[T]) Value() T   { var v T; return v }

type iterNone[T any] struct{}

func (it *iterNone[T]) Next() bool { return false }
func (it *iterNone[T]) Err() error { return nil }
func (it *iterNone[T]) Value() T   { var v T; return v }

type iterTrans[T, U any] struct {
	base  Iter[T]
	trans func(T) U
}

func (it *iterTrans[T, U]) Next() bool { return it.base.Next() }
func (it *iterTrans[T, U]) Err() error { return it.base.Err() }
func (it *iterTrans[T, U]) Value() U   { return it.trans(it.base.Value()) }

// IndexState is used by the indexer to specify fields for a given access
type IndexState interface {
	Use(constraint int)
	SelectIndex(int)
}

// Indexer allow types to implement their own index access method.
type Indexer interface {
	// The [IndexState] parameter will configure which constraints are passed to [Filter]:
	//   - the index number (arbitrary from the point of the of the driver) set by [IndexState.SelectIndex]
	//   - the arguments from columns in the order from [IndexState.Use]
	BestIndex(IndexState, Constraints) (estimatedCost, estimatedRows int64)
}

// Updater exposes types to modifications through SQL commands.
//
// Depending on the operation, not all fields are present:
//
//   - during an insert, only the value is set
//   - during a delete, only the index is set
//   - during an update, both the index and the value is set
//
// The first context carries the current connection, it must be used if the update requires writing to the DB.
type Updater[T any] interface {
	Hasher
	Update(_ context.Context, index int64, value T) error
}
