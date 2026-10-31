// Rewrites the fixed Ray agent ports in a KubeRay pod's `ray start` command to 0, so Ray nodes of several pods that
// share one VM's host network bind free ports instead of colliding.
package main

import "regexp"

// rayFixedPortFlag matches the `ray start` flags KubeRay sets to the same port in every pod: the metrics export port
// (8080), whose bind failure kills the dashboard agent and with it the raylet, and the dashboard agent's HTTP port
// (52365), whose bind failure disables the agent's HTTP API.
var rayFixedPortFlag = regexp.MustCompile(`(--(?:metrics-export-port|dashboard-agent-listen-port))(?:=|\s+)\d+\b`)

// rayFixedPortWord matches one of those flags alone in a word, whose value is the next word.
var rayFixedPortWord = regexp.MustCompile(`^--(?:metrics-export-port|dashboard-agent-listen-port)$`)

// portWord matches a word that is a port number.
var portWord = regexp.MustCompile(`^\d+$`)

// freeRayFixedPorts sets the fixed Ray agent ports in every command and argument of a KubeRay pod to 0. Every
// container on a VM shares the VM's network namespace and address, so a second Ray node there cannot bind a port the
// first holds; with 0, each agent binds a free port and records it in its node's GCS entry, where the head reads it.
func freeRayFixedPorts(pod *Pod) {
	if _, ok := pod.Metadata.Labels[rayNodeTypeLabel]; !ok {
		return
	}
	for i := range pod.Spec.Containers {
		c := &pod.Spec.Containers[i]
		for _, words := range [][]string{c.Command, c.Args} {
			for j, w := range words {
				words[j] = rayFixedPortFlag.ReplaceAllString(w, "${1}=0")
				if j > 0 && rayFixedPortWord.MatchString(words[j-1]) && portWord.MatchString(w) {
					words[j] = "0"
				}
			}
		}
	}
}
