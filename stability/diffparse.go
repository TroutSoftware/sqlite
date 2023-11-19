package stability

import (
	"strconv"
	"strings"
)

const (
	eof           = "\x00"
	linestartplus = "+++ "
	linestartarob = "@@ "
)

func adv(data, look string) (after, found string) {
	var ix int
	switch look {
	case linestartplus:
		ix = strings.Index(data, linestartplus)
		found = linestartplus
	case linestartarob:
		ix = strings.Index(data, linestartarob)
		found = linestartarob
		if jx := strings.Index(data, linestartplus); jx != -1 && jx < ix {
			ix = jx
			found = linestartplus
		}
	}

	if ix == -1 {
		return "", eof
	}

	return data[ix+len(found):], found
}

type parser struct {
	hunks []Hunk

	st string
	lx string
}

func (p *parser) word() string {
	ix := strings.IndexAny(p.lx, " \t,")
	if ix > 0 {
		return p.lx[:ix]
	}
	return ""
}

func (p *parser) skipto(delim string) {
	ix := strings.Index(p.lx, delim)
	if ix > 0 {
		p.lx = p.lx[ix+len(delim):]
	}
}

func (p *parser) parseFile() {
	h := Hunk{Filename: p.word()}
	p.lx, p.st = adv(p.lx, linestartarob)
	for p.st == linestartarob {
		p.skipto("+")
		s, _ := strconv.ParseInt(p.word(), 10, 32)
		p.lx = p.lx[len(p.word())+1:]
		c, _ := strconv.ParseInt(p.word(), 10, 32)

		p.skipto("\n")
		h.Edits = append(h.Edits, int(s), int(s+c))
		p.lx, p.st = adv(p.lx, p.st)
	}

	p.hunks = append(p.hunks, h)
}

func peek(s string) string {
	if len(s) > 15 {
		return s[:15] + "(...)"
	}
	return s
}

func (p *parser) parseFiles(dt []byte) []Hunk {
	p.lx, p.st = adv(string(dt), linestartplus)
	for p.st != eof {
		p.parseFile()
	}
	return p.hunks
}

type Hunk struct {
	Filename string
	Edits    []int // odd for start, even for end
}
