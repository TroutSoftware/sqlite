package main

import (
	"github.com/TroutSoftware/sqlite/stability"
	"golang.org/x/tools/go/analysis/unitchecker"
)

func main() {
	unitchecker.Main(stability.ExplicitSerialized)
}
