//go:build !linux

package iosched

import (
	"errors"

	"github.com/miretskiy/dio/mempool"
)

// IOUringAvailable reports whether io_uring is supported.
const IOUringAvailable = false

// URingScheduler is declared so code referencing the type compiles on non-Linux.
type URingScheduler struct{}

// NewURingScheduler always returns an error on non-Linux platforms.
func NewURingScheduler(_ URingConfig) (*URingScheduler, error) {
	return nil, errors.New("iosched: io_uring requires Linux")
}

func (s *URingScheduler) Submit(_ Op) (*Ticket, error) {
	return nil, errors.New("iosched: io_uring requires Linux")
}

func (s *URingScheduler) Close() error { return nil }

func (s *URingScheduler) usePool(_ *mempool.SlabPool) error {
	return errors.New("iosched: io_uring requires Linux")
}
