//go:build windows

package scanner

import (
	"context"
	"os"
	"path/filepath"
	"testing"
)

func TestFollowSymlinkUsesTargetIdentityAndPreventsCycle(t *testing.T) {
	root := t.TempDir()
	target := filepath.Join(root, "target")
	if err := os.Mkdir(target, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(target, "file"), []byte("data"), 0o644); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(root, "link")
	if err := os.Symlink(target, link); err != nil {
		t.Skipf("directory symlinks unavailable: %v", err)
	}
	if err := os.Symlink(root, filepath.Join(target, "cycle")); err != nil {
		t.Skipf("directory symlink cycle unavailable: %v", err)
	}

	identities := make(map[string]string)
	count := 0
	err := (Scanner{Recursive: true, SymlinkPolicy: FollowSymlinks}).Scan(context.Background(), root, func(entry Entry) error {
		count++
		identities[entry.Path] = entry.Identity
		if count > 20 {
			t.Fatal("symlink cycle was not bounded")
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if identities[link] == "" || identities[target] == "" || identities[link] != identities[target] {
		t.Fatalf("followed link identity %q does not match target %q", identities[link], identities[target])
	}
}
