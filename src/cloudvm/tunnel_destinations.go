// Parses the per-namespace destinations that worker pods may reach through the tunnel, Services and external hosts, and
// derives the synthetic addresses and redirects that let worker processes dial external hosts without a proxy.
package main

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"net"
	"net/netip"
	"sort"
	"strconv"
	"strings"

	"k8s.io/apimachinery/pkg/util/validation"
)

// syntheticHostNetwork is the RFC 2544 benchmarking range from which workers' agents answer the allow-listed external
// hostnames; no real destination uses it, so the bootstrap can redirect all of it into the agent.
var syntheticHostNetwork = netip.MustParsePrefix("198.18.0.0/15")

// NamespaceDestinations lists, per pod namespace, the cluster Services and the external host:port destinations that
// worker pods of the namespace may reach through the tunnel besides their own namespace's Services.
type NamespaceDestinations struct {
	services map[string]map[string]bool
	hosts    map[string]map[string]bool
}

// ParseNamespaceDestinations parses a JSON object mapping a namespace to a list of namespace/service and host:port
// entries; an empty string configures none.
func ParseNamespaceDestinations(raw string) (*NamespaceDestinations, error) {
	d := &NamespaceDestinations{services: map[string]map[string]bool{}, hosts: map[string]map[string]bool{}}
	if strings.TrimSpace(raw) == "" {
		return d, nil
	}
	var byNamespace map[string][]string
	if err := json.Unmarshal([]byte(raw), &byNamespace); err != nil {
		return nil, fmt.Errorf("invalid namespace destinations: want a JSON object of namespace to destination list: %w", err)
	}
	for namespace, entries := range byNamespace {
		if errs := validation.IsDNS1123Label(namespace); len(errs) > 0 {
			return nil, fmt.Errorf("invalid namespace %q in namespace destinations: %s", namespace, strings.Join(errs, "; "))
		}
		for _, entry := range entries {
			if err := d.add(namespace, strings.TrimSpace(entry)); err != nil {
				return nil, fmt.Errorf("invalid destination %q of namespace %s: %w", entry, namespace, err)
			}
		}
	}
	return d, nil
}

func (d *NamespaceDestinations) add(namespace, entry string) error {
	if svcNamespace, name, ok := strings.Cut(entry, "/"); ok {
		if len(validation.IsDNS1123Label(svcNamespace)) > 0 || len(validation.IsDNS1035Label(name)) > 0 {
			return fmt.Errorf("want namespace/service")
		}
		addTo(d.services, namespace, entry)
		return nil
	}
	host, port, err := net.SplitHostPort(entry)
	if err != nil {
		return fmt.Errorf("want namespace/service or host:port: %w", err)
	}
	if p, err := strconv.Atoi(port); err != nil || p < 1 || p > 65535 {
		return fmt.Errorf("port %q is outside 1-65535", port)
	}
	if addr, err := netip.ParseAddr(host); err == nil {
		if !addr.Is4() {
			return fmt.Errorf("only IPv4 addresses are supported")
		}
		if syntheticHostNetwork.Contains(addr) {
			return fmt.Errorf("address is inside %s, which workers use for external hostnames", syntheticHostNetwork)
		}
		addTo(d.hosts, namespace, addr.String()+":"+port)
		return nil
	}
	host = strings.ToLower(host)
	if len(validation.IsDNS1123Subdomain(host)) > 0 || !strings.Contains(host, ".") {
		return fmt.Errorf("host %q is neither an IPv4 address nor a fully qualified DNS name", host)
	}
	addTo(d.hosts, namespace, host+":"+port)
	return nil
}

func addTo(sets map[string]map[string]bool, key, value string) {
	if sets[key] == nil {
		sets[key] = map[string]bool{}
	}
	sets[key][value] = true
}

// HasServices reports whether any namespace lists a Service.
func (d *NamespaceDestinations) HasServices() bool {
	return len(d.services) > 0
}

// AllowsService reports whether pods of namespace may reach the Service svcNamespace/name.
func (d *NamespaceDestinations) AllowsService(namespace, svcNamespace, name string) bool {
	return d != nil && d.services[namespace][svcNamespace+"/"+name]
}

// AllowsHost reports whether pods of namespace may reach the external host:port target, a DNS name or an IPv4 address.
func (d *NamespaceDestinations) AllowsHost(namespace, host, port string) bool {
	if d == nil {
		return false
	}
	if addr, err := netip.ParseAddr(host); err == nil {
		host = addr.String()
	}
	return d.hosts[namespace][strings.ToLower(host)+":"+port]
}

// hostEntries returns every distinct external host:port destination of every namespace, sorted.
func (d *NamespaceDestinations) hostEntries() []string {
	seen := map[string]bool{}
	var out []string
	if d != nil {
		for _, hosts := range d.hosts {
			for entry := range hosts {
				if !seen[entry] {
					seen[entry] = true
					out = append(out, entry)
				}
			}
		}
	}
	sort.Strings(out)
	return out
}

// SyntheticHosts maps every allow-listed external hostname of every namespace to the address in syntheticHostNetwork
// that workers' agents answer it with. A hostname keeps its address while the set of hostnames that hash to the same
// slot stays the same, so a configuration change rarely moves one.
func (d *NamespaceDestinations) SyntheticHosts() map[string]string {
	names := map[string]bool{}
	for _, entry := range d.hostEntries() {
		host, _, _ := net.SplitHostPort(entry)
		if _, err := netip.ParseAddr(host); err != nil {
			names[host] = true
		}
	}
	if len(names) == 0 {
		return nil
	}
	sorted := make([]string, 0, len(names))
	for name := range names {
		sorted = append(sorted, name)
	}
	sort.Strings(sorted)
	base := binary.BigEndian.Uint32(syntheticHostNetwork.Addr().AsSlice())
	size := uint32(1) << (32 - syntheticHostNetwork.Bits())
	taken := map[uint32]bool{}
	out := make(map[string]string, len(sorted))
	for _, name := range sorted {
		sum := sha256.Sum256([]byte(name))
		// Offsets 0 and size-1 stay unused, so no answer is the network or broadcast address.
		offset := 1 + binary.BigEndian.Uint32(sum[:4])%(size-2)
		for taken[offset] {
			offset = 1 + offset%(size-2)
		}
		taken[offset] = true
		var ip [4]byte
		binary.BigEndian.PutUint32(ip[:], base+offset)
		out[name] = netip.AddrFrom4(ip).String()
	}
	return out
}

// ExternalAddresses returns the allow-listed host:port destinations whose host is an IPv4 address, sorted.
func (d *NamespaceDestinations) ExternalAddresses() []string {
	var out []string
	for _, entry := range d.hostEntries() {
		host, _, _ := net.SplitHostPort(entry)
		if _, err := netip.ParseAddr(host); err == nil {
			out = append(out, entry)
		}
	}
	return out
}

// TunnelRedirect is one destination whose TCP connections a worker's bootstrap redirects into its agent: a network on
// every port, or on Port when it is set.
type TunnelRedirect struct {
	Network string `json:"network"`
	Port    int    `json:"port,omitempty"`
}

// Redirects returns what workers redirect into their agent so the external destinations go through the tunnel: the
// synthetic hostname network when any hostname is allow-listed, and each allow-listed address on its port.
func (d *NamespaceDestinations) Redirects() []TunnelRedirect {
	var out []TunnelRedirect
	if len(d.SyntheticHosts()) > 0 {
		out = append(out, TunnelRedirect{Network: syntheticHostNetwork.String()})
	}
	for _, entry := range d.ExternalAddresses() {
		host, port, _ := net.SplitHostPort(entry)
		p, _ := strconv.Atoi(port)
		out = append(out, TunnelRedirect{Network: host + "/32", Port: p})
	}
	return out
}
