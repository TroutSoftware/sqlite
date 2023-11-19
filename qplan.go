package sqlite

import (
	"context"
	"fmt"
	"strings"
	"unsafe"
)

/*
#include <amalgamation/sqlite3.h>
#include <stdint.h>

extern void go_free(void*);
*/
import "C"

// IsExplain returns true iff the statement is an [explain query plan] command.
// This is the most important interface for debugging.
//
// [explain query plan]: https://www.sqlite.org/eqp.html
func (r *Rows) IsExplain() bool { return C.sqlite3_stmt_isexplain(r.stmt.s) == 2 }

// QueryPlan returns the [query plan] of the existing statement under a graphical form.
//
// [query plan]: https://www.sqlite.org/eqp.html
func (r *Rows) ExplainQueryPlan() string {
	if r.stmt.s == nil {
		return "<error>: calling query plan from a terminated query"
	}

	cq := C.sqlite3_expanded_sql(r.stmt.s)
	q := C.GoString(cq)
	C.go_free(unsafe.Pointer(cq))

	if C.sqlite3_stmt_isexplain(r.stmt.s) != 2 {
		q = fmt.Sprintf("explain query plan %s", q)
	}

	var buf strings.Builder

	type qpn struct {
		sib, chl *qpn
		thread   bool

		node int
		desc string
	}

	var root qpn

	rows := r.stmt.c.Exec(r.ctx, q)
	for rows.Next() {
		var node, parent int
		var unused int
		var desc string
		rows.Scan(&node, &parent, &unused, &desc)
		p := &root
		for p.node != parent {
			if p.chl != nil {
				p = p.chl
			} else {
				p = p.sib
			}
			if p == &root {
				panic("node not found!")
			}
		}

		if p.chl == nil {
			p.chl = &qpn{
				node:   node,
				desc:   desc,
				sib:    p,
				thread: true,
			}
		} else {
			p.chl = &qpn{
				node: node,
				desc: desc,
				sib:  p.chl,
			}
		}
	}

	indent := " " // empty space OK for repr, and allow for p != &root
	for p := root.chl; p != nil && p != &root; {
		fmt.Fprintf(&buf, "%s %s\n", indent, p.desc)
		if p.chl != nil {
			indent += "*"
			p = p.chl
			continue
		}
		for p.thread {
			indent = indent[:len(indent)-1]
			p = p.sib
		}
		p = p.sib
	}

	if rows.Err() != nil {
		return fmt.Sprintf("<error>: %s", rows.Err())
	}

	return buf.String()
}

// Bytecodes outputs the detailed query plan through the SQLite [bytecode] operands.
// This is a fairly low-level operation, and is only really useful for troubleshooting.
// Even then, [Rows.ExplainQueryPlan] is usually a much better option.
//
// [bytecode]: https://www.sqlite.org/opcode.html
func (r *Rows) Bytecode() string {
	if r.stmt.s == nil {
		return "<error>: calling bytecode plan from a terminated query"
	}

	type bytecode struct {
		addr    int
		opcode  string
		p1      int
		p2      int
		p3      int
		p4      string
		p5      int
		comment string
		subprog string
	}
	var buf strings.Builder
	buf.WriteString("\naddr\topcode\t\tp1\tp2\tp3\tp4\t\tcomment\n")
	buf.WriteString("---\t---\t\t--\t--\t--\t---\t---\n")

	rows := r.stmt.c.Exec(context.Background(), `select addr, opcode, p1, p2, p3, p4, comment from bytecode(?)`, r.stmt.s)
	for rows.Next() {
		var b bytecode
		rows.Scan(&b.addr, &b.opcode, &b.p1, &b.p2, &b.p3, &b.p4, &b.comment)
		fmt.Fprintf(&buf, "%d\t%s\t%d\t%d\t%d\t%s\t%s\n", b.addr, expandto(b.opcode, 12), b.p1, b.p2, b.p3, expandto(b.p4, 12), b.comment)
	}

	if rows.Err() != nil {
		return fmt.Sprintf("<error>: %s", rows.Err())
	}

	return buf.String()
}

func expandto(s string, w int) string {
	dst := make([]byte, w)
	for i := range dst {
		dst[i] = ' '
	}
	copy(dst, s)
	if len(s) > w {
		const ellipsis = "(…)"
		copy(dst[len(dst)-len(ellipsis):], ellipsis)
	}

	return string(dst)
}
