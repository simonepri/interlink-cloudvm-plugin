// Answers cluster DNS queries that worker agents send over their tunnel sessions, using the plugin pod's own resolver.
package main

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"strings"
	"time"

	"github.com/hashicorp/yamux"
	"golang.org/x/net/dns/dnsmessage"
)

// WorkerDNSStreamKind names a stream that carries one DNS query, which the worker ends by closing its side, and the
// gateway's reply.
const WorkerDNSStreamKind = "dns"

// DefaultClusterDomain is the DNS domain of cluster services when the gateway configuration names none.
const DefaultClusterDomain = "cluster.local"

const (
	// maxDNSQuery bounds a worker's query; real queries, even with EDNS options, stay far below it.
	maxDNSQuery = 4096
	// maxDNSReply is the largest message DNS over TCP can carry.
	maxDNSReply = 65535
	// dnsStreamTimeout bounds one dns stream from its header to the reply, including every resolver tried.
	dnsStreamTimeout = 10 * time.Second
	// dnsExchangeTimeout bounds one exchange with one cluster resolver.
	dnsExchangeTimeout = 4 * time.Second
	podResolvConf      = "/etc/resolv.conf"
)

// serveWorkerDNS answers the one query a worker-opened dns stream carries; it closes the stream without a reply when
// the query is malformed or no cluster resolver answers, and the agent then answers its client with SERVFAIL.
func (g *TunnelGateway) serveWorkerDNS(instanceID string, reader *bufio.Reader, stream *yamux.Stream) {
	if err := stream.SetDeadline(time.Now().Add(dnsStreamTimeout)); err != nil {
		return
	}
	query, err := io.ReadAll(io.LimitReader(reader, maxDNSQuery+1))
	if err != nil || len(query) > maxDNSQuery {
		return
	}
	reply, err := g.answerWorkerDNS(instanceID, query)
	if err != nil {
		log.Printf("[cloudvm-tunnel] Cannot answer dns query from %s: %v", sanitizeLog(instanceID), err) //nolint:gosec // sanitized
		return
	}
	_, _ = stream.Write(reply)
}

// answerWorkerDNS answers a worker's query with the endpoints of a regional stand-in when one serves the worker's
// region and has endpoints, and otherwise as answerClusterDNS does.
func (g *TunnelGateway) answerWorkerDNS(instanceID string, query []byte) ([]byte, error) {
	if reply, ok := g.answerFromStandIn(instanceID, query); ok {
		return reply, nil
	}
	return g.answerClusterDNS(query)
}

// answerFromStandIn answers an A query for <service>.<namespace>.svc.<domain> from a worker whose pods are in
// namespace with the addresses of the stand-in for that Service in the worker's region. It reports false, so the
// query goes to the real Service, when there is no stand-in or the stand-in has no ready endpoint.
func (g *TunnelGateway) answerFromStandIn(instanceID string, query []byte) ([]byte, bool) {
	if g.regionalServices == nil || g.workerRegion == nil || g.workerNamespace == nil {
		return nil, false
	}
	header, question, err := parseDNSQuery(query)
	if err != nil || question.Type != dnsmessage.TypeA || question.Class != dnsmessage.ClassINET {
		return nil, false
	}
	name, namespace, ok := serviceOfDNSName(question.Name.String(), g.clusterDomain)
	if !ok {
		return nil, false
	}
	if workerNamespace, ok := g.workerNamespace(instanceID); !ok || workerNamespace != namespace {
		return nil, false
	}
	region, ok := g.workerRegion(instanceID)
	if !ok {
		return nil, false
	}
	standIn, ok := g.regionalServices.StandIn(namespace, name, region)
	if !ok {
		return nil, false
	}
	addrs := g.resolveA(fmt.Sprintf("%s.%s.svc.%s.", standIn.Name, standIn.Namespace, strings.Trim(g.clusterDomain, ".")), header.ID)
	if len(addrs) == 0 {
		return nil, false
	}
	reply, err := dnsAReply(header, question, addrs)
	if err != nil {
		return nil, false
	}
	return reply, true
}

// serviceOfDNSName splits a <service>.<namespace>.svc.<domain> name into its Service and namespace.
func serviceOfDNSName(fqdn, domain string) (name, namespace string, ok bool) {
	fqdn = strings.ToLower(strings.TrimSuffix(fqdn, "."))
	suffix := ".svc." + strings.ToLower(strings.Trim(domain, "."))
	rest, found := strings.CutSuffix(fqdn, suffix)
	if !found {
		return "", "", false
	}
	name, namespace, found = strings.Cut(rest, ".")
	if !found || name == "" || namespace == "" || strings.Contains(namespace, ".") {
		return "", "", false
	}
	return name, namespace, true
}

// resolveA asks the cluster resolvers for the A records of fqdn and returns them; it returns none when no resolver
// answers NOERROR.
func (g *TunnelGateway) resolveA(fqdn string, id uint16) [][4]byte {
	qname, err := dnsmessage.NewName(fqdn)
	if err != nil {
		return nil
	}
	b := dnsmessage.NewBuilder(nil, dnsmessage.Header{ID: id, RecursionDesired: true})
	if b.StartQuestions() != nil || b.Question(dnsmessage.Question{Name: qname, Type: dnsmessage.TypeA, Class: dnsmessage.ClassINET}) != nil {
		return nil
	}
	query, err := b.Finish()
	if err != nil {
		return nil
	}
	for _, server := range g.clusterDNSServers {
		reply, err := exchangeDNS(server, query, id)
		if err != nil {
			continue
		}
		var msg dnsmessage.Message
		if msg.Unpack(reply) != nil || msg.RCode != dnsmessage.RCodeSuccess {
			return nil
		}
		var addrs [][4]byte
		for _, answer := range msg.Answers {
			if a, ok := answer.Body.(*dnsmessage.AResource); ok {
				addrs = append(addrs, a.A)
			}
		}
		return addrs
	}
	return nil
}

// dnsAReply builds a reply to a query that answers its question with addrs.
func dnsAReply(query dnsmessage.Header, question dnsmessage.Question, addrs [][4]byte) ([]byte, error) {
	b := dnsmessage.NewBuilder(nil, dnsmessage.Header{
		ID: query.ID, Response: true, Authoritative: true, OpCode: query.OpCode, RecursionDesired: query.RecursionDesired,
		RecursionAvailable: true,
	})
	if err := b.StartQuestions(); err != nil {
		return nil, err
	}
	if err := b.Question(question); err != nil {
		return nil, err
	}
	if err := b.StartAnswers(); err != nil {
		return nil, err
	}
	for _, addr := range addrs {
		h := dnsmessage.ResourceHeader{Name: question.Name, Type: dnsmessage.TypeA, Class: dnsmessage.ClassINET, TTL: standInTTL}
		if err := b.AResource(h, dnsmessage.AResource{A: addr}); err != nil {
			return nil, err
		}
	}
	return b.Finish()
}

// answerClusterDNS sends a query for a name inside the cluster domain to the cluster resolvers in turn and returns the
// first reply; it answers a query for any other name with REFUSED.
func (g *TunnelGateway) answerClusterDNS(query []byte) ([]byte, error) {
	header, question, err := parseDNSQuery(query)
	if err != nil {
		return nil, err
	}
	if !inDNSDomain(question.Name.String(), g.clusterDomain) {
		return dnsErrorReply(header, question, dnsmessage.RCodeRefused)
	}
	if len(g.clusterDNSServers) == 0 {
		return nil, errors.New("no cluster DNS server is configured")
	}
	for _, server := range g.clusterDNSServers {
		var reply []byte
		reply, err = exchangeDNS(server, query, header.ID)
		if err == nil {
			return reply, nil
		}
	}
	return nil, err
}

// parseDNSQuery returns the header and single question of a standard query.
func parseDNSQuery(msg []byte) (dnsmessage.Header, dnsmessage.Question, error) {
	var p dnsmessage.Parser
	header, err := p.Start(msg)
	if err != nil {
		return header, dnsmessage.Question{}, err
	}
	if header.Response || header.OpCode != 0 {
		return header, dnsmessage.Question{}, errors.New("message is not a standard query")
	}
	questions, err := p.AllQuestions()
	if err != nil {
		return header, dnsmessage.Question{}, err
	}
	if len(questions) != 1 {
		return header, dnsmessage.Question{}, fmt.Errorf("query has %d questions, want 1", len(questions))
	}
	return header, questions[0], nil
}

// inDNSDomain reports whether a name equals domain or lies below it, ignoring case and trailing dots.
func inDNSDomain(name, domain string) bool {
	name = strings.ToLower(strings.TrimSuffix(name, "."))
	domain = strings.ToLower(strings.Trim(domain, "."))
	return domain != "" && (name == domain || strings.HasSuffix(name, "."+domain))
}

// dnsErrorReply builds a reply to a query that carries only its question and the given response code.
func dnsErrorReply(query dnsmessage.Header, question dnsmessage.Question, rcode dnsmessage.RCode) ([]byte, error) {
	b := dnsmessage.NewBuilder(nil, dnsmessage.Header{
		ID: query.ID, Response: true, OpCode: query.OpCode, RecursionDesired: query.RecursionDesired, RCode: rcode,
	})
	if err := b.StartQuestions(); err != nil {
		return nil, err
	}
	if err := b.Question(question); err != nil {
		return nil, err
	}
	return b.Finish()
}

// exchangeDNS sends query to server over UDP, repeating it over TCP when the UDP reply is truncated.
func exchangeDNS(server string, query []byte, id uint16) ([]byte, error) {
	reply, err := exchangeDNSUDP(server, query, id)
	if err != nil {
		return nil, err
	}
	var p dnsmessage.Parser
	if header, err := p.Start(reply); err == nil && header.Truncated {
		return exchangeDNSTCP(server, query, id)
	}
	return reply, nil
}

func exchangeDNSUDP(server string, query []byte, id uint16) ([]byte, error) {
	conn, err := net.DialTimeout("udp", server, dnsExchangeTimeout)
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	if err := conn.SetDeadline(time.Now().Add(dnsExchangeTimeout)); err != nil {
		return nil, err
	}
	if _, err := conn.Write(query); err != nil {
		return nil, err
	}
	buf := make([]byte, maxDNSReply)
	for {
		n, err := conn.Read(buf)
		if err != nil {
			return nil, err
		}
		// A reply to another query, such as a late answer to an earlier one, is skipped.
		if n >= 2 && binary.BigEndian.Uint16(buf) == id {
			return buf[:n], nil
		}
	}
}

func exchangeDNSTCP(server string, query []byte, id uint16) ([]byte, error) {
	conn, err := net.DialTimeout("tcp", server, dnsExchangeTimeout)
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	if err := conn.SetDeadline(time.Now().Add(dnsExchangeTimeout)); err != nil {
		return nil, err
	}
	if err := writeDNSMessage(conn, query); err != nil {
		return nil, err
	}
	reply, err := readDNSMessage(bufio.NewReader(conn), maxDNSReply)
	if err != nil {
		return nil, err
	}
	if binary.BigEndian.Uint16(reply) != id {
		return nil, errors.New("reply does not match the query")
	}
	return reply, nil
}

// readDNSMessage reads one message prefixed by its two-byte length, refusing messages longer than limit.
func readDNSMessage(r io.Reader, limit int) ([]byte, error) {
	var size [2]byte
	if _, err := io.ReadFull(r, size[:]); err != nil {
		return nil, err
	}
	n := int(binary.BigEndian.Uint16(size[:]))
	if n < 12 || n > limit {
		return nil, fmt.Errorf("dns message of %d bytes is outside 12-%d", n, limit)
	}
	msg := make([]byte, n)
	if _, err := io.ReadFull(r, msg); err != nil {
		return nil, err
	}
	return msg, nil
}

// writeDNSMessage writes msg prefixed by its two-byte length.
func writeDNSMessage(w io.Writer, msg []byte) error {
	if len(msg) > maxDNSReply {
		return fmt.Errorf("dns message of %d bytes exceeds %d", len(msg), maxDNSReply)
	}
	_, err := w.Write(append(binary.BigEndian.AppendUint16(nil, uint16(len(msg))), msg...)) //nolint:gosec // bounded above
	return err
}

// ResolvConfNameservers returns the nameservers a resolv.conf file lists, as host:port addresses on port 53.
func ResolvConfNameservers(path string) []string {
	data, err := os.ReadFile(path) //nolint:gosec // fixed system path or test fixture
	if err != nil {
		return nil
	}
	var servers []string
	for _, line := range strings.Split(string(data), "\n") {
		fields := strings.Fields(line)
		if len(fields) >= 2 && fields[0] == "nameserver" {
			servers = append(servers, net.JoinHostPort(fields[1], "53"))
		}
	}
	return servers
}
