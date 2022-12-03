package utils

import "time"
import "golang.org/x/exp/constraints"

type Number interface {
	constraints.Integer | constraints.Float
}

func MB[T Number](bytes T) float64 {
	return float64(bytes) / 1000000.0
}

func MBPS[T Number](bytes T, dur time.Duration) float64 {
	return MB(bytes) / dur.Seconds()
}
