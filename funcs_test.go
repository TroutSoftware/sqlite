package sqlite

import (
	"context"
	"errors"
	"testing"
)

func TestFunc(t *testing.T) {
	type MyString struct{ s string }

	db, err := Open(":memory:",
		RegisterFunc("greetings", func(v string) string { return v + " world" }),
		RegisterFunc("count_bytes", func(v []byte) int { return len(v) }),
		RegisterFunc("my_add", func(v, w int) int { return v + w }),
		RegisterFunc("my_onlyerror", func() error { return errors.New("onlyerror") }),
		RegisterFunc("my_nevererror", func() (int, error) { return 42, nil }),
		RegisterFunc("my_alwayserror", func() (int, error) { return 0, errors.New("alwayserror") }),
		RegisterFunc("readmypointer", func(v *MyString) { v.s = "yes sir!" }),
		RegisterFunc("passmypointer", func(v *MyString) PointerValue { return AsPointer(v) }),
		RegisterFunc("add_all", func(vals ...int) int {
			sum := 0
			for _, v := range vals {
				sum += v
			}
			return sum
		}),
	)
	if err != nil {
		t.Fatal(err)
	}

	t.Run("greetings", func(t *testing.T) {
		var out string
		err := db.Exec(context.Background(), "select greetings('hello')").ScanOne(&out)
		if err != nil {
			t.Fatal(err)
		}

		if out != "hello world" {
			t.Error("invalid string", out)
		}
	})

	t.Run("count_bytes", func(t *testing.T) {
		var count int
		err := db.Exec(context.Background(), "select count_bytes(?)", []byte("hello")).ScanOne(&count)
		if err != nil {
			t.Fatal(err)
		}

		if count != 5 {
			t.Error("invalid bytes count", count)
		}
	})

	t.Run("my_add", func(t *testing.T) {
		var add int
		err := db.Exec(context.Background(), "select my_add(40, 2)").ScanOne(&add)
		if err != nil {
			t.Fatal(err)
		}

		if add != 42 {
			t.Error("invalid bytes count", add)
		}
	})

	t.Run("my_onlyerror", func(t *testing.T) {
		err := db.Exec(context.Background(), "select my_onlyerror()").Err()
		if !errors.Is(err, errors.New("onlyerror")) {
			t.Error("invalid error", err)
		}
	})

	t.Run("my_nevererror", func(t *testing.T) {
		var val int
		err := db.Exec(context.Background(), "select my_nevererror()").ScanOne(&val)
		if err != nil {
			t.Fatal(err)
		}

		if val != 42 {
			t.Error("invalid value returned", val)
		}
	})

	t.Run("my_alwayserror", func(t *testing.T) {
		var val int
		err := db.Exec(context.Background(), "select my_alwayserror()").ScanOne(&val)
		if !errors.Is(err, errors.New("alwayserror")) {
			t.Error("invalid error", err)
		}

		if val != 0 {
			t.Error("invalid value returned", val)
		}
	})

	t.Run("readmypointer", func(t *testing.T) {
		var val MyString
		err := db.Exec(context.Background(), "select readmypointer(?)", AsPointer(&val)).Err()
		if err != nil {
			t.Fatal(err)
		}

		if val.s != "yes sir!" {
			t.Error("invalid read back", val.s)
		}
	})

	t.Run("passmypointer", func(t *testing.T) {
		var val MyString
		err := db.Exec(context.Background(), "select readmypointer(passmypointer(?))", AsPointer(&val)).Err()
		if err != nil {
			t.Fatal(err)
		}

		if val.s != "yes sir!" {
			t.Error("invalid read back", val.s)
		}
	})

	t.Run("varargs", func(t *testing.T) {
		var sum int
		err := db.Exec(context.Background(), "select add_all(1, 2, 3, 4, 5)").ScanOne(&sum)
		if err != nil {
			t.Fatal(err)
		}
		if sum != 15 {
			t.Error("bad sum", sum)
		}
	})
}

/*
func exportAllFuncs(t *testing.T, db *Conn) {
	var funcs []string
	st := db.Exec(context.Background(), "pragma function_list")
	for st.Next() {
		var fname string
		st.Scan(&fname)
		funcs = append(funcs, fname)
	}
	if st.Err() != nil {
		t.Fatal(st.Err())
	}

	sort.Strings(funcs)
	for _, n := range funcs {
		t.Log("function", n)
	}

}
*/
