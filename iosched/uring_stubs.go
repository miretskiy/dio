//go:build !linux

package iosched

import "errors"

// IOUringAvailable reports whether io_uring is supported.
// Always false on non-Linux platforms.
const IOUringAvailable = false

// NewURingScheduler always returns an error on non-Linux platforms.
func NewURingScheduler(_ URingConfig) (*URingScheduler, error) {
	return nil, errors.New("iosched: io_uring requires Linux")
}

// URingScheduler is declared here so that code referencing the type compiles
// on non-Linux. The struct is intentionally empty and unexported-inaccessible
// since NewURingScheduler always errors.
type URingScheduler struct{}

func (s *URingScheduler) ReadAt(_ int, _ []byte, _ int64) (int, error) {
	return 0, errors.New("iosched: io_uring requires Linux")
}

func (s *URingScheduler) WriteAt(_ int, _ []byte, _ int64) (int, error) {
	return 0, errors.New("iosched: io_uring requires Linux")
}

func (s *URingScheduler) Stats() Stats { return Stats{} }

func (s *URingScheduler) Close() error { return nil }
