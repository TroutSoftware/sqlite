package main

import (
	"golang.org/x/tools/go/analysis/unitchecker"
	"github.com/TroutSoftware/sqlite/stability"
)

func main() {
	unitchecker.Main(stability.ExplicitSerialized)
}
