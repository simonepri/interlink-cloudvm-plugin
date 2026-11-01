// Embeds the in-VM traffic meter, which tunnel workers fetch next to the runner, and its bootstrap settings.
package main

import (
	_ "embed"
	"fmt"
)

// trafficMeterScript is the in-VM traffic meter the runner imports to count the VM's and each pod's bytes by peer.
//
//go:embed traffic_meter.py
var trafficMeterScript string

// TrafficMeterScriptSHA256 returns the hex sha256 a tunnel worker fetches and verifies the traffic meter by.
func (c *CloudInitConfig) TrafficMeterScriptSHA256() string {
	return scriptSHA256(trafficMeterScript)
}

// TrafficProxyPorts returns the tunnel agent's loopback proxy ports, whose connections are a pod's tunnel traffic.
func (c *CloudInitConfig) TrafficProxyPorts() string {
	return fmt.Sprintf("%d,%d", tcpProxyPort, transparentProxyPort)
}
