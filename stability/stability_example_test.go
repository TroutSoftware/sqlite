package stability_test

import "github.com/TroutSoftware/sqlite/stability"

type User struct {
	_ stability.SerializedValue

	Name string
	Age  int
}

func Example() {
	// empty function, the User structure matters.
}