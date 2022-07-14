// just an example mobile app.
// changes the screen color everytime a unicast message is received
package main

import (
	"context"

	"golang.org/x/mobile/app"
	"golang.org/x/mobile/event/lifecycle"
	"golang.org/x/mobile/event/paint"
	"golang.org/x/mobile/event/size"
	"golang.org/x/mobile/gl"

	"github.com/libp2p/go-libp2p"
	"github.com/piax/go-byzskip/ayame"
	bs "github.com/piax/go-byzskip/byzskip"
)

var (
	ok = false
)

const (
	// XXX hard coded.
	KEY        = 1
	INTRODUCER = "/ip4/your-introducer-ip/udp/9000/quic/your-introducer-id"
)

func main() {
	app.Main(func(a app.App) {
		var glctx gl.Context
		sz := size.Event{}
		go startNode(a)
		for {
			e := <-a.Events()
			switch e := a.Filter(e).(type) {
			case lifecycle.Event:
				glctx, _ = e.DrawContext.(gl.Context)
			case size.Event:
				sz = e
			case paint.Event:
				if glctx == nil {
					continue
				}
				onDraw(glctx, sz)
				a.Publish()
			}
		}
	})
}

func startNode(a app.App) {
	h, _ := libp2p.New(libp2p.ListenAddrStrings("/ip4/0.0.0.0/udp/9000/quic"))
	node, _ := bs.New(h, bs.Key(ayame.IntKey(KEY)))
	introducer := INTRODUCER

	node.Join(context.Background(), introducer)
	node.SetMessageReceiver(func(node *bs.BSNode, ev *bs.BSUnicastEvent) {
		ok = !ok // inverse
		a.Send(paint.Event{})
	})
}

func onDraw(glctx gl.Context, sz size.Event) {
	if ok {
		glctx.ClearColor(0, 1, 0, 1) // green
	} else {
		glctx.ClearColor(1, 0, 0, 1) // red
	}
	glctx.Clear(gl.COLOR_BUFFER_BIT)
}
