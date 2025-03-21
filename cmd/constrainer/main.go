// Constrainer is a tool to generate acces methods from virtual tables types definitions.
// This reduces toil in what is mostly a routine work.
//
// The code follows the semantics of the `vtab` mini-language, and makes sure that:
//
//   - required fields have an access method with the right type
//   - filtered fields have an access method with two forms (T, bool)
//   - non-filtered fields do not have an access method
package main

import (
	"bytes"
	"flag"
	"fmt"
	"go/ast"
	"go/format"
	"go/token"
	"go/types"
	"log"
	"os"
	"reflect"
	"strings"
	"text/template"

	"golang.org/x/tools/go/packages"
)

var tpl = template.Must(template.New("").Parse(`
	func (r {{.VTable}}) Get{{.Field}}(cs sqlite.Constraints) (v {{.RType}}, {{if not .Required}}ok bool{{end}}) {
		match := 0
		for _, c := range cs {
			if c.Column == "{{.Column}}" {
				if c.Operation == sqlite.ConstraintEQ {
					{{- if .Accessor}}
					v = c.{{.Accessor}}()
					{{- else}}
					v = c.Value.({{.RType}})
					{{- end}}
					match++
				} else {
					panic("Value getter with non-constrained values")
				}
			}
		}
		{{if .Required}}
		if match != 1 {
			panic("field should have been filtered")
		}

		return v
		{{- else -}}
		if match == 0 {
			return v, false
		}
		if match == 1 {
			return v, true
		}

		panic("more than one match")
		{{- end -}}
	}
	`))

type accessor struct {
	VTable   string
	Field    string
	Column   string
	RType    string
	Accessor string

	Required bool
}

func main() {
	flag.Parse()
	typename := flag.Arg(0)

	pkg, err := packages.Load(&packages.Config{
		Fset:  token.NewFileSet(),
		Mode:  packages.NeedName | packages.NeedTypes | packages.NeedTypesInfo | packages.NeedSyntax,
		Tests: false,
	}, ".")
	if err != nil {
		log.Fatal(err)
	}

	var buf bytes.Buffer
	fmt.Fprint(&buf, "// Code generated by the constrainer; DO NOT EDIT.\n//go:build linux\n\n")

	imports := map[string]struct{}{
		"github.com/TroutSoftware/sqlite": {},
	}
	var gens []accessor
	var loc token.Position

	for _, pkg := range pkg {
		for _, f := range pkg.Syntax {
			ast.Inspect(f, func(n ast.Node) bool {
				ts, ok := n.(*ast.TypeSpec)
				if !ok {
					return true
				}
				stdef, ok := pkg.TypesInfo.Defs[ts.Name].Type().Underlying().(*types.Struct)
				if !ok {
					return true
				}

				if ts.Name.Name != typename {
					return true
				}
				loc = pkg.Fset.Position(n.Pos())

				fmt.Fprintf(&buf, "package %s\n", pkg.Name)

				for i := range stdef.NumFields() {

					tags := strings.Split(reflect.StructTag(stdef.Tag(i)).Get("vtab"), ",")
					acc := accessor{
						VTable: ts.Name.Name,
						Field:  stdef.Field(i).Name(),
						Column: tags[0],
					}
					include := false
					for _, v := range tags[1:] {
						if v == "required" {
							acc.Required = true
							include = true
						}
						if v == "filtered" || v == "hidden" {
							include = true
						}
					}
					if !include {
						continue
					}

					qname := types.TypeString(stdef.Field(i).Type(), rel(pkg.Types))
					if pkgname(stdef.Field(i).Type()) != pkg.PkgPath {
						imports[pkgname(stdef.Field(i).Type())] = struct{}{}
					}

					acc.RType = qname
					switch qname {
					default:
					case "bool":
						acc.Accessor = "ValueBool"
					case "[]byte":
						acc.Accessor = "ValueBytes"
					case "string":
						acc.Accessor = "ValueString"
					case "int":
						acc.Accessor = "ValueInt"
					}

					gens = append(gens, acc)
				}

				return true
			})
		}
	}

	if len(gens) == 0 {
		fmt.Fprintf(os.Stderr, "<warning>: no fields are `required`, `filtered` or `hidden` in type %s (in %s). No accessors will be generated.\n", typename, loc)
		return
	}

	fmt.Fprint(&buf, "import (\n")
	for i := range imports {
		if i == "" {
			continue
		}
		fmt.Fprintf(&buf, "%q\n", i)
	}
	fmt.Fprint(&buf, ")\n")

	for _, g := range gens {
		if err := tpl.Execute(&buf, g); err != nil {
			log.Fatal(err)
		}
	}

	out, err := format.Source(buf.Bytes())
	if err != nil {
		log.Fatalf("invalid code generated: %s\n\n%s", err, buf.Bytes())
	}

	if err := os.WriteFile(strings.ToLower(typename)+"_access.go", out, 0644); err != nil {
		log.Fatal("cannot write generated file", err)
	}
}

func rel(pkg *types.Package) types.Qualifier {
	if pkg == nil {
		return nil
	}
	return func(other *types.Package) string {
		if pkg == other {
			return "" // same package; unqualified
		}
		return other.Name()
	}
}

func pkgname(t types.Type) string {
	switch t := t.(type) {
	case *types.Pointer:
		return pkgname(t.Elem())
	case *types.Named:
		return t.Obj().Pkg().Path()
	case *types.Basic:
		return ""
	case *types.Slice:
		return pkgname(t.Elem())
	default:
		panic("not implemented" + t.String())
	}
}
