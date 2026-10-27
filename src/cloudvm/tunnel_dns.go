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
	reply, err := g.answerClusterDNS(query)
	if err != nil {
		log.Printf("[cloudvm-tunnel] Cannot answer dns query from %s: %v", sanitizeLog(instanceID), err) //nolint:gosec // sanitized
		return
	}
	_, _ = stream.Write(reply)
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
