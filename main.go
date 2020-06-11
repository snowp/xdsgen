package main

import (
	"fmt"
	"github.com/envoyproxy/go-control-plane/pkg/cache/types"
	"github.com/envoyproxy/go-control-plane/pkg/cache/v3"
	"github.com/snowp/xdsgen/xds"
)

func main() {
	g, err := xds.SnapshotGenerator()
	if err != nil {
		panic(err)
	}

	for o := range g.Out {
		snapshot := o.Value.(cache.Snapshot)
		fmt.Println(len(snapshot.Resources[types.Endpoint].Items))
		fmt.Printf("%v\n", o)
	}
}
