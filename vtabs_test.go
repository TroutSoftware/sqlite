package sqlite

import (
	"context"
	"encoding/json"
	"hash/maphash"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
)

func TestMetadata(t *testing.T) {

	t.Run("byte value", func(t *testing.T) {
		t.Skip("https://www.notion.so/trout-software/Starlark-Connectors-07b0a638896741b8b8e2196c81b23055")
		type S1 struct {
			Data []byte `vtab:"data"`
		}
		strdata := `{coins: {"penny": 1, "nickel": 5, "dime": 10, "quarter": 25}, i: 109, string: "hola"}`
		b, err := json.Marshal(strdata)
		if err != nil {
			t.Fatal(err)
		}
		s1 := S1{Data: b}

		db, err := Open(t.TempDir()+"/db", RegisterConstant("connector", s1))
		if err != nil {
			t.Fatal(err)
		}

		var got S1
		err = db.Exec(context.Background(), "select data from connector").ScanOne(&got.Data)
		if err != nil {
			t.Fatal("error", err)
		}
		t.Log(string(got.Data))
		var output string
		if err := json.Unmarshal(got.Data, &output); err != nil {
			t.Fatal("error", err)
		}

		if strdata != output {
			t.Errorf("invalid read back: want %v, got %v", strdata, output)
		}
	})

	t.Run("simple values", func(t *testing.T) {
		type S1 struct {
			Name string `vtab:"name"`
			Age  int    `vtab:"age"`
		}
		s1 := S1{Name: "John Doe", Age: 42}

		db, err := Open(t.TempDir()+"/db", RegisterConstant("users", s1))
		if err != nil {
			t.Fatal(err)
		}

		var got S1
		err = db.Exec(context.Background(), "select name, age from users").ScanOne(&got.Name, &got.Age)
		if err != nil {
			t.Fatal("error", err)
		}

		if got != s1 {
			t.Errorf("invalid read back: want %v, got %v", s1, got)
		}
	})

	t.Run("required value", func(t *testing.T) {
		type S1 struct {
			Name string `vtab:"name,required"`
			Age  int    `vtab:"age"`
		}
		s1 := S1{Name: "John Doe", Age: 42}

		db, err := Open(t.TempDir()+"/db", RegisterConstant("users", s1))
		if err != nil {
			t.Fatal(err)
		}

		var got S1
		err = db.Exec(context.Background(), "select name, age from users").ScanOne(&got.Name, &got.Age)
		if err == nil {
			t.Error("expected constraint violation, got nothing")
		}
		t.Log(err)
	})

	t.Run("skipped value", func(t *testing.T) {
		type S1 struct {
			Name   string `vtab:"name"`
			Age    int    `vtab:"-"`
			Gender string `vtab:"gender"`
		}
		s1 := S1{Name: "John Doe", Age: 42, Gender: "male"}

		db, err := Open(t.TempDir()+"/db", RegisterConstant("users", s1))
		if err != nil {
			t.Fatal(err)
		}

		var got S1
		err = db.Exec(context.Background(), "select name, gender from users").ScanOne(&got.Name, &got.Gender)
		if err != nil {
			t.Fatal("error", err)
		}

		if !cmp.Equal(s1, got, cmpopts.IgnoreFields(S1{}, "Age")) {
			t.Errorf("invalid read back: want %v, got %v", s1, got)
		}
	})

	t.Run("JSON accepted", func(t *testing.T) {
		type S1 struct {
			Name json.RawMessage `vtab:"name"`
		}
		s1 := S1{Name: []byte(`{"hello": "world"}`)}

		db, err := Open(t.TempDir()+"/db", RegisterConstant("users", s1))
		if err != nil {
			t.Fatal(err)
		}

		var got string
		err = db.Exec(context.Background(), "select name->'hello' from users").ScanOne(&got)
		if err != nil {
			t.Fatal("error", err)
		}

		if got != `"world"` {
			t.Error("got invalid JSON back", got)
		}
	})

	t.Run("Bool accepted", func(t *testing.T) {
		type S1 struct {
			OK bool `vtab:"ok"`
		}
		s1 := S1{OK: true} // false by init otherwise

		db, err := Open(t.TempDir()+"/db", RegisterConstant("bools", s1))
		if err != nil {
			t.Fatal(err)
		}

		var got bool
		err = db.Exec(context.Background(), "select ok from bools").ScanOne(&got)
		if err != nil {
			t.Fatal("error", err)
		}

		if !got {
			t.Error("got invalid boolean back", got)
		}
	})
}

func TestFilter(t *testing.T) {
	extract := make(chan []Constraint, 1) // extra space to prevent blocking
	db, err := Open(t.TempDir()+"/db", RegisterTable("filter", filterValue{cs: extract}))
	if err != nil {
		t.Fatal(err)
	}

	lexsort := cmpopts.SortSlices(func(i, j Constraint) bool { return i.Column < j.Column })
	onlypub := cmpopts.IgnoreUnexported(Constraint{})

	t.Run("text", func(t *testing.T) {
		err := db.Exec(context.Background(), "select count(*) from filter where text='hello'").Err()
		if err != nil {
			t.Fatal(err)
		}

		cs := <-extract
		want := []Constraint{{Value: "hello", Operation: ConstraintEQ, Column: "text"}}
		if !cmp.Equal(cs, want, onlypub) {
			t.Error(cmp.Diff(cs, want, onlypub))
		}
	})

	t.Run("number", func(t *testing.T) {
		err := db.Exec(context.Background(), "select count(*) from filter where num=42").Err()
		if err != nil {
			t.Fatal(err)
		}

		cs := <-extract
		want := []Constraint{{Value: 42, Operation: ConstraintEQ, Column: "num"}}
		if !cmp.Equal(cs, want, onlypub) {
			t.Error(cmp.Diff(cs, want, onlypub))
		}
	})

	t.Run("blob", func(t *testing.T) {
		err := db.Exec(context.Background(), "select count(*) from filter where blob=?", []byte("hellobytes")).Err()
		if err != nil {
			t.Fatal(err)
		}

		cs := <-extract
		want := []Constraint{{Value: []byte("hellobytes"), Operation: ConstraintEQ, Column: "blob"}}
		if !cmp.Equal(cs, want, onlypub) {
			t.Error(cmp.Diff(cs, want, onlypub))
		}
	})

	t.Run("multiple values", func(t *testing.T) {
		err := db.Exec(context.Background(), "select count(*) from filter where text=? and num < 3", "hello world").Err()
		if err != nil {
			t.Fatal(err)
		}

		cs := <-extract
		want := []Constraint{
			{Value: "hello world", Operation: ConstraintEQ, Column: "text"},
			{Value: 3, Operation: ConstraintLT, Column: "num"},
		}
		if !cmp.Equal(cs, want, lexsort, onlypub) {
			t.Error(cmp.Diff(cs, want, lexsort, onlypub))
		}
	})

	t.Run("multiple values in same column", func(t *testing.T) {
		err := db.Exec(context.Background(), "select count(*) from filter where text=? and text = 'other'", "hello world").Err()
		if err != nil {
			t.Fatal(err)
		}
	})

	t.Run("exclude limit", func(t *testing.T) {
		err := db.Exec(context.Background(), "select count(*) from filter where text='needme' limit 10").Err()
		if err != nil {
			t.Fatal(err)
		}

		cs := <-extract
		want := []Constraint{
			{Value: "needme", Operation: ConstraintEQ, Column: "text"},
		}
		if !cmp.Equal(cs, want, onlypub) {
			t.Error(cmp.Diff(cs, want, onlypub))
		}
	})
}

type filterValue struct {
	Primary string `vtab:"text,filtered"`
	Num     int    `vtab:"num,filtered"`
	Blob    []byte `vtab:"blob,filtered"`

	cs chan []Constraint `vtab:"-"`
}

func (f filterValue) Filter(_ int, cs Constraints) Iter[filterValue] {
	f.cs <- cs // capture for testing
	return &iterone[filterValue]{v: f}
}

func (f filterValue) BestIndex(st IndexState, cs Constraints) (ecost, erow int64) {
	for i := range cs {
		st.Use(i)
	}
	return 1, 1
}

func TestLeftJoin(t *testing.T) {
	db, err := Open(":memory:", RegisterTable("p", LeftJoinS1{}), RegisterTable("q", LeftJoinS2{}))
	if err != nil {
		t.Fatal(err)
	}
	type person struct{ Name, BirthPlace string }

	st := db.Exec(context.Background(), `explain select q.name, birth_place from p left join q using (name) order by name`)
	for st.Next() {
		var res MultiString
		st.Scan(&res)
		t.Log("query plan", res)
	}
	if st.Err() != nil {
		t.Fatal("query plan error", st.Err())
	}

	rows := db.Exec(context.Background(), `select q.name, birth_place from p left join q using (name) order by name`)

	var people []person
	for rows.Next() {
		var p person
		rows.Scan(&p.Name, &p.BirthPlace)
		people = append(people, p)
	}
	if rows.Err() != nil {
		t.Fatal(rows.Err())
	}
	want := []person{{"Carson", "Z"}, {"Martin", "Y"}, {"Octavio", "X"}}
	if !cmp.Equal(people, want) {
		t.Errorf("got different results back %s", cmp.Diff(people, want))
	}
}

type LeftJoinS1 struct {
	Name string `vtab:"name"`
}

func (s LeftJoinS1) Filter(_ int, cs Constraints) Iter[LeftJoinS1] {
	return FromArray([]LeftJoinS1{{Name: "Octavio"}, {Name: "Martin"}, {Name: "Carson"}})
}

type LeftJoinS2 struct {
	Name       string `vtab:"name,required"`
	BirthPlace string `vtab:"birth_place"`
}

// manually expanded template from constrainer (avoid circular import)
func (r LeftJoinS2) GetName(cs Constraints) (v string) {
	match := 0
	for _, c := range cs {
		if c.Column == "name" {
			if c.Operation == ConstraintEQ {
				v = c.ValueString()
				match++
			} else {
				panic("Value getter with non-constrained values")
			}
		}
	}
	if match != 1 {
		panic("field should have been filtered")
	}

	return v
}

func (s LeftJoinS2) Filter(_ int, cs Constraints) Iter[LeftJoinS2] {
	var birthPlace string
	name := s.GetName(cs)
	switch name {
	case "Octavio":
		birthPlace = "X"
	case "Martin":
		birthPlace = "Y"
	case "Carson":
		birthPlace = "Z"
	}
	return FromOne(LeftJoinS2{
		Name:       name,
		BirthPlace: birthPlace,
	})
}

func TestPointerPassing(t *testing.T) {
	db, err := Open(":memory:",
		RegisterTable("pointed", Rich{}),
		RegisterFunc("printme", func(p *PtrTo) string {
			t.Log("got pointer", p)
			return p.v
		}),
	)
	if err != nil {
		t.Fatal(err)
	}

	var plan strings.Builder
	stn := db.Exec(context.Background(), "explain select printme(ptr) from pointed(?)", AsPointer(&PtrTo{v: "hello, world!"}))
	for stn.Next() {
		var line MultiString
		stn.Scan(&line)
		t.Log("write lin", line)
		plan.WriteString(strings.Join(line, "|") + "\n")
	}
	if stn.Err() != nil {
		t.Fatal(stn.Err())
	}
	t.Log("plan is ", plan.String())

	var got string
	if err := db.Exec(context.Background(), "select printme(ptr) from pointed(?)", AsPointer(&PtrTo{v: "hello, world!"})).ScanOne(&got); err != nil {
		t.Fatal(err)
	}

	if got != "hello, world!" {
		t.Error("invalid pointer passed ", got)
	}
}

type PtrTo struct{ v string }
type Rich struct {
	Ptr *PtrTo `vtab:"ptr,required,hidden"`
}

func (r Rich) Filter(_ int, cs Constraints) Iter[Rich] {
	var ptr *PtrTo
	for _, cs := range cs {
		if cs.Column == "ptr" {
			ptr = cs.Value.(*PtrTo)
		}
	}
	if ptr == nil {
		panic("invalid")
	}

	return &iterone[Rich]{v: Rich{Ptr: ptr}}
}

// https://sqlite.org/vtab.html#xrowid must return the same number if the values are the same
// by default, we have a counter per connection, so every row ID is considered different
// implementations might have a specific Hash method to override this behavior
func TestRowID(t *testing.T) {
	db, err := Open(":memory:",
		RegisterTable("stringsize", StringSize{}))
	if err != nil {
		t.Fatal(err)
	}

	type ss struct {
		S string
		D int
	}
	var got []ss

	if err := db.Exec(context.Background(), "PRAGMA vdbe_trace = true").Err(); err != nil {
		t.Fatal(err)
	}
	rows := db.Exec(context.Background(), `select value, size from stringsize where value in (?, 'wworld');`, "hello")
	t.Log(rows.ExplainQueryPlan())
	t.Log(rows.Bytecode())
	for rows.Next() {
		var v string
		var sz int
		rows.Scan(&v, &sz)
		if len(v) != sz {
			t.Errorf("%s has invalid size %d", v, sz)
		}
		got = append(got, ss{v, sz})
	}

	if rows.Err() != nil {
		t.Fatal(rows.Err())
	}

	want := []ss{
		{"hello", 5},
		{"wworld", 6},
	}

	if !cmp.Equal(want, got) {
		t.Error(cmp.Diff(want, got))
	}
}

type StringSize struct {
	Value string `vtab:"value,filtered"`
	Size  int    `vtab:"size,filtered"`
}

func (a StringSize) Filter(_ int, cs Constraints) Iter[StringSize] {
	for _, c := range cs {
		if c.Column == "value" {
			val := c.Value.(string)
			return FromOne(StringSize{Value: val, Size: len(val)})
		}
		if c.Column == "size" {
			sz := c.Value.(int)
			val := strings.Repeat("a", sz)
			return FromOne(StringSize{Value: val, Size: sz})
		}
	}
	return FromOne(StringSize{})
}

var sseed = maphash.MakeSeed()

func (a StringSize) Hash64() int64 { return int64(maphash.String(sseed, a.Value)) }

func TestUpdate(t *testing.T) {
	db, err := Open(":memory:",
		RegisterTable("test", Standard{}))
	if err != nil {
		t.Fatal(err)
	}

	scenario := [...]struct {
		query string
		want  []Standard
	}{
		{"insert into test (key, value) values ('hello', 'world')", []Standard{{"hello", "world"}}},
		{"update test set value = 'other world' where key = 'hello'", []Standard{{"hello", "other world"}}},
		{"insert into test (key, value) values ('well', 'done')", []Standard{{"hello", "other world"}, {"well", "done"}}},
		{"delete from test", nil},
	}

	for i, s := range scenario {
		if err := db.Exec(context.Background(), s.query).Err(); err != nil {
			t.Fatalf("at step %d: %s", i, err)
		}

		var got []Standard
		for st := db.Exec(context.Background(), "select key, value from test"); st.Next(); {
			var s Standard
			st.Scan(&s.Key, &s.Value)
			got = append(got, s)
		}

		if err != nil {
			t.Fatalf("cannot read data back: %s", err)
		}

		if !cmp.Equal(got, s.want) {
			t.Errorf("at step %d: %s", i, cmp.Diff(got, s.want))
		}
	}

}

var GlobalStandards []Standard

type Standard struct {
	Key   string `vtab:"key"`
	Value string `vtab:"value"`
}

func (s Standard) Filter(_ int, cs Constraints) Iter[Standard] {
	return FromArray(GlobalStandards)
}

func (s Standard) Hash64() int64 { return int64(maphash.String(sseed, s.Key)) }

func (s Standard) Update(_ context.Context, idx int64, n Standard) error {
	for i, v := range GlobalStandards {
		ix := int64(maphash.String(sseed, v.Key))
		if idx != -1 && idx == ix {
			if n.Key != "" || n.Value != "" {
				GlobalStandards[i] = n
			} else {
				GlobalStandards = append(GlobalStandards[:i], GlobalStandards[i+1:]...)
			}
			return nil
		}
	}

	GlobalStandards = append(GlobalStandards, n)
	return nil
}
