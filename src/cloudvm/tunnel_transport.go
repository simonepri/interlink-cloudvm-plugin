// Package main implements the interlink-cloudvm-plugin daemon.
// This file selects how each region's workers reach the tunnel gateway (tailnet, router, public) and checks the instance identity of public-transport workers.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net"
	"sort"
	"strings"
)

// TunnelTransport names the network path a region's workers take to the tunnel gateway.
type TunnelTransport string

const (
	// TunnelTransportTailnet makes workers join the tailnet and dial the private gateway hostname.
	TunnelTransportTailnet TunnelTransport = "tailnet"
	// TunnelTransportRouter makes workers dial the gateway ClusterIP that the footprint VPC routes through its router pair.
	TunnelTransportRouter TunnelTransport = "router"
	// TunnelTransportPublic makes workers dial the public listener and prove their identity with the AWS instance identity document.
	TunnelTransportPublic TunnelTransport = "public"
)

// ParseTunnelTransport validates a transport name; an empty name means tailnet.
func ParseTunnelTransport(raw string) (TunnelTransport, error) {
	switch t := TunnelTransport(strings.TrimSpace(raw)); t {
	case "":
		return TunnelTransportTailnet, nil
	case TunnelTransportTailnet, TunnelTransportRouter, TunnelTransportPublic:
		return t, nil
	default:
		return "", fmt.Errorf("unknown tunnel transport %q (want tailnet, router or public)", raw)
	}
}

// TunnelTransports maps regions to their transport; regions without an entry use tailnet.
type TunnelTransports map[string]TunnelTransport

// For returns the transport of region.
func (t TunnelTransports) For(region string) TunnelTransport {
	if tr, ok := t[region]; ok {
		return tr
	}
	return TunnelTransportTailnet
}

// Uses reports whether any listed region uses transport.
func (t TunnelTransports) Uses(transport TunnelTransport) bool {
	return len(t.regionsUsing(transport)) > 0
}

func (t TunnelTransports) regionsUsing(transport TunnelTransport) []string {
	var regions []string
	for region, tr := range t {
		if tr == transport {
			regions = append(regions, region)
		}
	}
	sort.Strings(regions)
	return regions
}

// TunnelTransportsFromClusterFacts reads regions.<region>.tunnelTransport from the CLUSTER_FACTS JSON.
func TunnelTransportsFromClusterFacts(raw string) (TunnelTransports, error) {
	transports := TunnelTransports{}
	if strings.TrimSpace(raw) == "" {
		return transports, nil
	}
	var facts ClusterFacts
	if err := json.Unmarshal([]byte(raw), &facts); err != nil {
		return nil, fmt.Errorf("invalid CLUSTER_FACTS: %w", err)
	}
	for region, fact := range facts.Regions {
		tr, err := ParseTunnelTransport(fact.TunnelTransport)
		if err != nil {
			return nil, fmt.Errorf("CLUSTER_FACTS region %s: %w", region, err)
		}
		transports[region] = tr
	}
	return transports, nil
}

// validateTransports fails when a transport some region uses lacks the settings it needs.
func (s *TunnelSettings) validateTransports() error {
	if regions := s.Transports.regionsUsing(TunnelTransportRouter); len(regions) > 0 {
		if s.GatewayIP == "" {
			return fmt.Errorf("regions %v use the router tunnel transport but TUNNEL_GATEWAY_IP is not set", regions)
		}
		if net.ParseIP(s.GatewayIP) == nil {
			return fmt.Errorf("TUNNEL_GATEWAY_IP %q is not an IP address", s.GatewayIP)
		}
	}
	if regions := s.Transports.regionsUsing(TunnelTransportPublic); len(regions) > 0 {
		if s.PublicEndpoint == "" {
			return fmt.Errorf("regions %v use the public tunnel transport but TUNNEL_PUBLIC_ENDPOINT is not set", regions)
		}
		if _, port, err := net.SplitHostPort(s.PublicEndpoint); err != nil || port == "" {
			return fmt.Errorf("TUNNEL_PUBLIC_ENDPOINT %q must be host:port", s.PublicEndpoint)
		}
	}
	return nil
}

// meshKeyRequired reports whether some launch may need the tailnet key: every unlisted region uses tailnet.
func (t TunnelTransports) meshKeyRequired() bool {
	return len(t) == 0 || t.Uses(TunnelTransportTailnet)
}

// TunnelIdentityVerificationEnv set to "disabled" lets public-transport workers in on their launch token alone.
const TunnelIdentityVerificationEnv = "TUNNEL_IDENTITY_VERIFICATION"

// publicIdentityVerifier picks the verifier for public-transport handshakes: the AWS verifier, unless verification is disabled.
func publicIdentityVerifier(server *Server, mode string, allowedAccounts []string) (AttestationVerifier, error) {
	switch strings.TrimSpace(mode) {
	case "disabled":
		log.Printf("[cloudvm-tunnel] %s=disabled: public-transport workers are authenticated by launch token only", TunnelIdentityVerificationEnv)
		return DisabledAttestationVerifier{}, nil
	case "", "enabled":
	default:
		return nil, fmt.Errorf("invalid %s %q (want enabled or disabled)", TunnelIdentityVerificationEnv, mode)
	}
	hasAccount := false
	for _, a := range allowedAccounts {
		if strings.TrimSpace(a) != "" {
			hasAccount = true
		}
	}
	if !hasAccount {
		return nil, fmt.Errorf("the public tunnel transport needs %s, or %s=disabled", TunnelAllowedAccountsEnv, TunnelIdentityVerificationEnv)
	}
	return NewAWSIdentityVerifier(allowedAccounts, server.LaunchRecord)
}

// DisabledAttestationVerifier accepts every document; the gateway uses it while instance identity verification is disabled.
type DisabledAttestationVerifier struct{}

var _ AttestationVerifier = DisabledAttestationVerifier{}

// VerifyAWS accepts the document without checking it.
func (DisabledAttestationVerifier) VerifyAWS(context.Context, []byte) (*AWSInstanceIdentity, error) {
	return &AWSInstanceIdentity{}, nil
}

// VerifyGCP accepts the token without checking it.
func (DisabledAttestationVerifier) VerifyGCP(context.Context, string, string) (*GCPInstanceIdentity, error) {
	return &GCPInstanceIdentity{}, nil
}

// ValidateTunnelIdentity requires a verified AWS instance identity document from workers launched with the public transport.
// A worker the plugin no longer tracks must present one whenever any region uses the public transport.
func (s *Server) ValidateTunnelIdentity(ctx context.Context, req *TunnelHandshakeRequest) error {
	t := s.tunnelConfig()
	if t == nil || req == nil {
		return nil
	}
	if region := s.nodeRegion(req.InstanceID); region != "" {
		if t.Transports.For(region) != TunnelTransportPublic {
			return nil
		}
	} else if !t.Transports.Uses(TunnelTransportPublic) {
		return nil
	}
	if len(req.Attestation) == 0 || !strings.EqualFold(req.Platform, "aws") {
		return errors.New("public tunnel transport requires the signed AWS instance identity document")
	}
	verifier := t.IdentityVerifier
	if verifier == nil {
		verifier = DisabledAttestationVerifier{}
	}
	identity, err := verifier.VerifyAWS(ctx, req.Attestation)
	if err != nil {
		return err
	}
	if identity.InstanceID != "" && identity.InstanceID != req.InstanceID {
		return fmt.Errorf("identity document names instance %s, handshake names %s", identity.InstanceID, req.InstanceID)
	}
	return nil
}

// tunnelHost returns the host part of a host:port endpoint.
func tunnelHost(endpoint string) string {
	if host, _, err := net.SplitHostPort(endpoint); err == nil {
		return host
	}
	return endpoint
}
