package utils

import "sync"

type BetterPool[T any] struct {
	pool  sync.Pool
	reset func(T)
}

func NewPool[T any](new func() T, reset func(T)) BetterPool[T] {
	return BetterPool[T]{
		pool:  sync.Pool{New: func() any { return new() }},
		reset: reset,
	}
}

func (p *BetterPool[T]) Get() T {
	return p.pool.Get().(T)
}

func (p *BetterPool[T]) Put(val T) {
	p.reset(val)
	p.pool.Put(val)
}
