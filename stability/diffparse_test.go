package stability

import (
	"testing"

	"github.com/google/go-cmp/cmp"
)

func TestParse(t *testing.T) {
	cases := []struct {
		in  string
		out []Hunk
	}{
		{`Index: hub/schedule.go
===================================================================
--- hub/schedule.go	(révision 2137)
+++ hub/schedule.go	(copie de travail)
@@ -15,0 +16 @@
+	"github.com/TroutSoftware/sqlite/stability"
@@ -18,0 +20,2 @@
+
+
@@ -19,0 +23,2 @@
+	_ stability.SerializedValue
+`, []Hunk{{"hub/schedule.go", []int{16, 16, 20, 22, 23, 25}}}},
	}

	for _, c := range cases {
		var p parser
		got := p.parseFiles([]byte(c.in))
		if !cmp.Equal(got, c.out) {
			t.Error(cmp.Diff(got, c.out))
		}
	}
}
