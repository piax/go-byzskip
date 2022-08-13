package main

import (
	"syscall/js"

	// not
	sim "github.com/piax/go-byzskip/sim"
	//sim "github.com/piax/go-byzskip/bssim"
)

func wasmNodes(this js.Value, inputs []js.Value) interface{} {
	k := inputs[0].Int()
	return sim.ConstructNodes(k)
}

func wasmUnicast(this js.Value, inputs []js.Value) interface{} {
	return sim.DoUnicast()
}

func main() {
	sim.RUN_AS_WASM = true
	sim.DoSim()

	js.Global().Set("nodes", js.FuncOf(wasmNodes))
	js.Global().Set("unicast", js.FuncOf(wasmUnicast))
	// Prevent main from exiting

	finishMain := js.Global().Get("finishMain")
	finishMain.Invoke()
	select {} // never end

}
