// Package main implements the interlink-cloudvm-plugin daemon.
// This file embeds the in-VM spot interruption watcher and names the log file the tunnel agent ships its notices from.
package main

import _ "embed"

// spotWatcherScript is the in-VM watcher that prints a JSON line for each spot interruption notice the metadata service posts.
//
//go:embed spot_watcher.py
var spotWatcherScript string

// spotWatcherLogPath is the file the bootstrap writes the watcher's output to.
const spotWatcherLogPath = "/var/log/cloudvm-spot-watcher.log"

// SpotWatcherScript returns the embedded in-VM spot interruption watcher source.
func (c *CloudInitConfig) SpotWatcherScript() string {
	return spotWatcherScript
}

// SpotWatcherLogPath returns the file the bootstrap writes the watcher's output to.
func (c *CloudInitConfig) SpotWatcherLogPath() string {
	return spotWatcherLogPath
}

// SpotWatcherLogFile returns the CLOUDVM_LOG_FILES entry that ships the watcher's output under the spot log source.
func (c *CloudInitConfig) SpotWatcherLogFile() string {
	return WorkerLogSourceSpot + "=" + spotWatcherLogPath
}
