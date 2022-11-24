package utils

import "time"

func MB(bytes int) float64 {
	return float64(bytes) / 1000000.0
}

func MBPS(bytes int, dur time.Duration) float64 {
	return MB(bytes) / dur.Seconds()
}
