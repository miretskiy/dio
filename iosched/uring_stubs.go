//go:build !linux

package iosched

import (
	"errors"

	"github.com/miretskiy/dio/v2/nvme"
)

// IOUringAvailable reports whether io_uring is supported.
const IOUringAvailable = false

// URingScheduler is declared so code referencing the type compiles on non-Linux.
type URingScheduler struct{}

type nvmeSplit struct{}

type rawOperation struct{}

func recordRawResult(*operation, int, error) {}

func nvmeCommandLimit(*nvme.Device) int { return 0 }

// NewURingScheduler always returns an error on non-Linux platforms.
func NewURingScheduler(_ ...Option) (*URingScheduler, error) {
	return nil, errors.New("iosched: io_uring requires Linux")
}

func (s *URingScheduler) Submit(_ Op) (Ticket, error) {
	return Ticket{}, errors.New("iosched: io_uring requires Linux")
}

func (s *URingScheduler) Close() error { return nil }
