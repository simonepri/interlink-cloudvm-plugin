//go:build !race

// Reports that the race detector is off, so the scale test can run its full measurement window.

package main

const raceEnabled = false
