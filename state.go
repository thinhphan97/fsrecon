package fsrecon

import (
	"errors"
	"io/fs"
	"time"
)

// FileID is an opaque, best-effort filesystem identity. A zero FileID means
// that the platform could not provide a stable identity.
type FileID struct{ value string }

func newFileID(value string) FileID { return FileID{value: value} }

// IsZero reports whether no identity is available.
func (id FileID) IsZero() bool { return id.value == "" }

// Equal reports whether two non-zero identities identify the same file.
func (id FileID) Equal(other FileID) bool {
	return id.value != "" && id.value == other.value
}

// MarshalText serializes the opaque identity for SnapshotStore implementations.
func (id FileID) MarshalText() ([]byte, error) { return []byte(id.value), nil }

// UnmarshalText restores an identity previously produced by MarshalText.
func (id *FileID) UnmarshalText(text []byte) error {
	if id == nil {
		return errors.New("fsrecon: unmarshal FileID into nil receiver")
	}
	id.value = string(text)
	return nil
}

// FileType is a platform-independent filesystem entry type.
type FileType uint8

const (
	FileTypeUnknown FileType = iota
	FileTypeRegular
	FileTypeDirectory
	FileTypeSymlink
	FileTypeOther
)

func (t FileType) String() string {
	names := [...]string{"UNKNOWN", "REGULAR", "DIRECTORY", "SYMLINK", "OTHER"}
	if int(t) >= len(names) {
		return "UNKNOWN"
	}
	return names[t]
}

// FileState is the metadata fsrecon observes for one filesystem entry.
type FileState struct {
	Path    string
	ID      FileID
	Type    FileType
	Size    int64
	ModTime time.Time
	Mode    fs.FileMode
}
