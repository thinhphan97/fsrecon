//go:build linux || darwin

package scanner

import (
	"fmt"
	"io/fs"
	"syscall"
)

func fileIdentity(_ string, info fs.FileInfo) (string, uint64, error) {
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return "", 0, nil
	}
	return fmt.Sprintf("%x:%x", uint64(stat.Dev), uint64(stat.Ino)), uint64(stat.Nlink), nil
}
