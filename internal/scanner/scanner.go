// Package scanner provides a streaming, policy-aware filesystem traversal.
package scanner

import (
	"context"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"time"
)

var ErrSymlink = errors.New("symlink rejected by policy")

type SymlinkPolicy uint8

const (
	IgnoreSymlinks SymlinkPolicy = iota
	ReportSymlinks
	FollowSymlinks
	RejectSymlinks
)

type Entry struct {
	Path     string
	Identity string
	Type     fs.FileMode
	Size     int64
	ModTime  time.Time
	Mode     fs.FileMode
	Links    uint64
}

type Scanner struct {
	Recursive     bool
	SymlinkPolicy SymlinkPolicy
	Filter        func(Entry) bool
}

// Scan streams entries below root to fn. The root directory itself is not
// emitted; when root is a file, that file is emitted.
func (s Scanner) Scan(ctx context.Context, root string, fn func(Entry) error) error {
	root = filepath.Clean(root)
	info, err := os.Lstat(root)
	if err != nil {
		return err
	}
	if !info.IsDir() {
		return s.emit(ctx, root, info, fn)
	}

	seen := make(map[string]struct{})
	if id, _, err := fileIdentity(root, info); err == nil && id != "" {
		seen[id] = struct{}{}
	}
	return s.walkDir(ctx, root, fn, seen)
}

func (s Scanner) walkDir(ctx context.Context, dir string, fn func(Entry) error, seen map[string]struct{}) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		return err
	}
	for _, dirEntry := range entries {
		if err := ctx.Err(); err != nil {
			return err
		}
		path := filepath.Join(dir, dirEntry.Name())
		info, err := os.Lstat(path)
		if err != nil {
			if errors.Is(err, fs.ErrNotExist) {
				continue // The filesystem changed during traversal.
			}
			return err
		}

		isLink := info.Mode()&fs.ModeSymlink != 0
		if isLink {
			switch s.SymlinkPolicy {
			case IgnoreSymlinks:
				continue
			case RejectSymlinks:
				return &fs.PathError{Op: "scan", Path: path, Err: ErrSymlink}
			case FollowSymlinks:
				info, err = os.Stat(path)
				if err != nil {
					if errors.Is(err, fs.ErrNotExist) {
						continue
					}
					return err
				}
			}
		}

		entry, err := makeEntry(path, info)
		if err != nil {
			return err
		}
		if s.Filter != nil && !s.Filter(entry) {
			continue
		}
		if err := fn(entry); err != nil {
			return err
		}
		if !s.Recursive || !info.IsDir() {
			continue
		}
		id, _, err := fileIdentity(path, info)
		if err != nil {
			return err
		}
		if id != "" {
			if _, ok := seen[id]; ok {
				continue
			}
			seen[id] = struct{}{}
		}
		if err := s.walkDir(ctx, path, fn, seen); err != nil {
			return err
		}
	}
	return nil
}

func (s Scanner) emit(ctx context.Context, path string, info fs.FileInfo, fn func(Entry) error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	entry, err := makeEntry(path, info)
	if err != nil {
		return err
	}
	if s.Filter != nil && !s.Filter(entry) {
		return nil
	}
	return fn(entry)
}

func makeEntry(path string, info fs.FileInfo) (Entry, error) {
	id, links, err := fileIdentity(path, info)
	if err != nil {
		return Entry{}, err
	}
	return Entry{
		Path: path, Identity: id, Type: info.Mode().Type(), Size: info.Size(),
		ModTime: info.ModTime(), Mode: info.Mode(), Links: links,
	}, nil
}
