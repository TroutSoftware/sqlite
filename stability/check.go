package stability

import (
	"errors"
	"fmt"
	"go/ast"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"time"

	"golang.org/x/tools/go/analysis"
	"golang.org/x/tools/go/analysis/passes/inspect"
	"golang.org/x/tools/go/ast/inspector"
)

var ExplicitSerialized = &analysis.Analyzer{
	Name:     "serialized",
	Doc:      `Find data structures that are marked as being serialized`,
	Requires: []*analysis.Analyzer{inspect.Analyzer, Changes},
	Run:      run_collectStructs,
}

const helpMessage = `
====================
🤖🤖 Serialization Alert 🤖🤖

A data structure persisted to the database is going to be altered by this commit.
This might mean that older records cannot be safely read (backward compatibility).
And that makes the robot sad…

In order to ensure that we keep happy customers 😃, write a small test, that:

 - defines inline copy of the old structure type
 - persist a value of that old type
 - read the value with the new type

This is an example to get you started:

func TestCanReadMyStructV1(t *testing.T) {
	type V1 struct {
		// copy original content here
	}
	v1 := V1 {
		// include some sample values
	}

	dt, err := cbor.Marshal(v1)
	if err != nil {
		t.Fatal(err)
	}

	var out %s
	if err := cbor.Unmarshal(&out, dt); err != nil {
		t.Fatal(err)
	}

	if !cmp.Equal(want, out) {
		t.Error(cmp.Diff(want, out))
	}
}

Once you are happy with the compatibility story, silence this warning with the command 📟:

date -Iseconds > $HOME/.local/state/.stability_last_check

====================

`

func run_collectStructs(pass *analysis.Pass) (any, error) {
	if checkOverwrite() {
		return nil, nil
	}

	inspect := pass.ResultOf[inspect.Analyzer].(*inspector.Inspector)
	hunks := pass.ResultOf[Changes].([]Hunk)

	structs := []ast.Node{(*ast.StructType)(nil), (*ast.TypeSpec)(nil)}
	var tname string

	inspect.Preorder(structs, func(node ast.Node) {
		if st, ok := node.(*ast.TypeSpec); ok {
			tname = st.Name.Name // capture via pre-order
			return
		}

		st := node.(*ast.StructType)
		for _, f := range st.Fields.List {

			// in struct X { _ pkg.Type }, pkg.Type is a qualified identifier, but represented as a selector expression
			sel, ok := f.Type.(*ast.SelectorExpr)
			if ok {

				qid := pass.TypesInfo.Uses[sel.Sel]
				if qid.Pkg().Path() == "github.com/TroutSoftware/sqlite/stability" && qid.Name() == "SerializedValue" {
					f := pass.Fset.File(node.Pos())

					for _, h := range hunks {
						if h.Filename == f.Name() {
							for i := 0; i < len(h.Edits); i += 2 {
								if h.Edits[i] < f.Line(node.End()) && f.Line(node.Pos()) < h.Edits[i+1] {
									pass.ReportRangef(st, helpMessage, tname)
								}
							}
						}
					}

				}

			}
		}
	})

	return nil, nil
}

var Changes = &analysis.Analyzer{
	Name:       "changed",
	Doc:        "Use VCS to know which lines have changed",
	Run:        run_commitdiffs,
	ResultType: reflect.TypeOf([]Hunk{}),
}

func run_commitdiffs(pass *analysis.Pass) (any, error) {
	df, err := exec.Command("svn", "diff", "-x", "-U0").Output()
	if err != nil {
		return nil, fmt.Errorf("cannot get difference: %w", err)
	}

	var p parser
	hunks := p.parseFiles(df)
	for i, h := range hunks {
		hunks[i].Filename, err = filepath.Abs(h.Filename)
		if err != nil {
			return nil, fmt.Errorf("cannot get absolute path for %s", h.Filename)
		}
	}

	return hunks, nil
}

func checkOverwrite() bool {
	lcheck := os.Getenv("XDG_STATE_HOME")
	if lcheck == "" {
		var err error
		lcheck, err = os.UserHomeDir()
		if err != nil {
			log.Fatal("cannot get home dir", err)
		}
		lcheck = filepath.Join(lcheck, ".local/state")
	}
	lcheck = filepath.Join(lcheck, ".stability_last_check")

	dt, err := os.ReadFile(lcheck)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		log.Fatalf("cannot read file content at %s: %s", lcheck, err)
	}

	if len(dt) == 0 {
		return false
	}

	spec := strings.TrimSpace(string(dt))
	p, err := time.Parse(time.RFC3339, spec)
	if err != nil {
		log.Fatalf("invalid time specification %s: %s", string(dt), err)
	}

	return time.Since(p) < 5*time.Minute
}
