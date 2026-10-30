//go:build race

// Reports that the race detector is on, so the scale test shortens its window to fit the CI time budget.

package main

const raceEnabled = true
