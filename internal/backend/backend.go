// Package backend defines the internal contract for native filesystem events.
package backend

import (
	"context"
	"errors"
	"time"
)

var ErrOverflow = errors.New("filesystem event queue overflow")

type Op uint8

const (
	OpCreate Op = 1 << iota
	OpWrite
	OpRemove
	OpRename
	OpChmod
)

type RawEvent struct {
	Path string
	Op   Op
	Time time.Time
}

type Backend interface {
	Start(ctx context.Context, root string) error
	Events() <-chan RawEvent
	Errors() <-chan error
	Close() error
}
