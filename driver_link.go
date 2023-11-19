// Time to compile SQLite is rather OK (~ 30 secs), but this is painful in an edit-test cycle.
// Instead set the `sharedlib` tag, and run generate, then do your work as usual.
// Attention to the C build tags when doing so…

//go:build sharedlib

package sqlite

// #cgo LDFLAGS: -L${SRCDIR}/amalgamation -lsqlite3
// #cgo LDFLAGS: -ldl -lm
import "C"
