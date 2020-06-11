package xds

import (
	"context"
	"fmt"
	core "github.com/envoyproxy/go-control-plane/envoy/config/core/v3"
	endpoint "github.com/envoyproxy/go-control-plane/envoy/config/endpoint/v3"
	"github.com/envoyproxy/go-control-plane/pkg/cache/types"
	"github.com/envoyproxy/go-control-plane/pkg/cache/v3"
	"github.com/snowp/xdsgen/graph"
	"net"
	"time"
)

func singleIpCla(ip string) *endpoint.ClusterLoadAssignment {
	return &endpoint.ClusterLoadAssignment{
		Endpoints:            []*endpoint.LocalityLbEndpoints{
			{
				LbEndpoints:          []*endpoint.LbEndpoint{
					{
						HostIdentifier: &endpoint.LbEndpoint_Endpoint{
							Endpoint: &endpoint.Endpoint{
								Address:              &core.Address{
									Address:              &core.Address_SocketAddress{
										SocketAddress: &core.SocketAddress{
											Protocol:             443,
											Address:              ip,
										},
									},
								},
							},
						},
					},
				},
			},
		},
	}
}

// TODO these kind of repetitive constructs aren't very well suited for field reflection (too much boiler plate). Consider
// providing a way to register/evaluate pipelines using more dynamic ways (maps + funcs?)
type googleClaGenerator struct {
	GoogleIp string `pipeline:"dns/google.com"`
}

func (g googleClaGenerator) Generate() interface{} {
	cla := singleIpCla(g.GoogleIp)
	cla.ClusterName = "google"
	return cla
}

type yahooClaGenerator struct {
	YahooIp string `pipeline:"dns/yahoo.com"`
}

func (y yahooClaGenerator) Generate() interface{} {
	cla := singleIpCla(y.YahooIp)
	cla.ClusterName = "yahoo"
	return cla
}


type snapshotGenerator struct {
	GoogleCla *endpoint.ClusterLoadAssignment `pipeline:"cla/google.com"`
	YahooCla *endpoint.ClusterLoadAssignment `pipeline:"cla/yahoo.com"`
}



func (s snapshotGenerator) Generate() interface{} {
	return cache.NewSnapshot("1", []types.Resource{s.GoogleCla, s.YahooCla}, nil, nil, nil, nil)
}

func registerDnsInput(generator *graph.Generator, hostname string) error {
	dnsOne, err := generator.RegisterInput("dns/"+hostname)
	if err != nil {
		return err
	}
	go func() {
		for {
			time.Sleep(5 * time.Second)
			ip, err := net.ResolveIPAddr("", hostname)
			if err != nil {
				fmt.Println(err)
				continue
			}

			dnsOne <- ip.IP.String()
		}
	}()

	return nil
}

func SnapshotGenerator() (*graph.Generator, error) {
	g := graph.NewGenerator(context.Background())

	err := registerDnsInput(&g, "google.com")
	if err != nil {
		return nil, err
	}
	err = registerDnsInput(&g, "yahoo.com")
	if err != nil {
		return nil, err
	}

	err = g.RegisterPipelineStage("cla/google.com", googleClaGenerator{}, false)
	if err != nil {
		return nil, err
	}

	err = g.RegisterPipelineStage("cla/yahoo.com", yahooClaGenerator{}, false)
	if err != nil {
		return nil, err
	}

	err = g.RegisterPipelineStage("snapshot", snapshotGenerator{}, true)
	if err != nil {
		return nil, err
	}


	return &g, g.Finalize()
}
