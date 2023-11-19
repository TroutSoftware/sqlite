// Package stability provides an API to validate the stability of persisted data structures.
// The only exported type is [SerializedValue], that must be included data structures for persistence.
//
// It checks that code change in data structures does not impact backward-compatibility.
package stability

// SerializedValue should be the first anonymous member of a data structure that is serialized (see example).
// Data structures marked as such can be passed to SQLite for persistence.
type SerializedValue struct{}
