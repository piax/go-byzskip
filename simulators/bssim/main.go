package main

import (
	// not
	sim "github.com/piax/go-byzskip/sim"
	//sim "github.com/piax/go-byzskip/bssim"
)

func main() {
	sim.RUN_AS_WASM = false
	sim.DoSim()
}
