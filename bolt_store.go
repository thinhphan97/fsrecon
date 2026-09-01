package fsrecon

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"path/filepath"
	"sync"
	"time"

	bolt "go.etcd.io/bbolt"
)

var snapshotBucket = []byte("fsrecon.snapshot.v1")

// BoltStore is a durable, transaction-backed SnapshotStore.
type BoltStore struct {
	mu     sync.RWMutex
	db     *bolt.DB
	closed bool
}

// OpenBoltStore opens or creates a persistent snapshot database.
func OpenBoltStore(path string) (*BoltStore, error) {
	if path == "" {
		return nil, errors.New("fsrecon: bolt store path is required")
	}
	db, err := bolt.Open(filepath.Clean(path), 0o600, &bolt.Options{Timeout: time.Second})
	if err != nil {
		return nil, fmt.Errorf("fsrecon: open bolt store: %w", err)
	}
	if err := db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists(snapshotBucket)
		return err
	}); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("fsrecon: initialize bolt store: %w", err)
	}
	return &BoltStore{db: db}, nil
}

func (s *BoltStore) Get(ctx context.Context, path string) (FileState, bool, error) {
	if err := ctx.Err(); err != nil {
		return FileState{}, false, err
	}
	db, err := s.database()
	if err != nil {
		return FileState{}, false, err
	}
	var state FileState
	found := false
	err = db.View(func(tx *bolt.Tx) error {
		value := tx.Bucket(snapshotBucket).Get([]byte(path))
		if value == nil {
			return nil
		}
		found = true
		return decodeFileState(value, &state)
	})
	return state, found, err
}

func (s *BoltStore) Put(ctx context.Context, state FileState) error {
	return s.Apply(ctx, []FileState{state}, nil)
}

func (s *BoltStore) Delete(ctx context.Context, path string) error {
	return s.Apply(ctx, nil, []string{path})
}

func (s *BoltStore) Apply(ctx context.Context, puts []FileState, deletes []string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	db, err := s.database()
	if err != nil {
		return err
	}
	return db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(snapshotBucket)
		for _, path := range deletes {
			if err := ctx.Err(); err != nil {
				return err
			}
			if err := bucket.Delete([]byte(path)); err != nil {
				return err
			}
		}
		for _, state := range puts {
			if err := ctx.Err(); err != nil {
				return err
			}
			value, err := encodeFileState(state)
			if err != nil {
				return err
			}
			if err := bucket.Put([]byte(state.Path), value); err != nil {
				return err
			}
		}
		return nil
	})
}

func (s *BoltStore) Walk(ctx context.Context, prefix string, fn func(FileState) error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	db, err := s.database()
	if err != nil {
		return err
	}
	return db.View(func(tx *bolt.Tx) error {
		cursor := tx.Bucket(snapshotBucket).Cursor()
		for key, value := cursor.Seek([]byte(prefix)); key != nil; key, value = cursor.Next() {
			if err := ctx.Err(); err != nil {
				return err
			}
			path := string(key)
			if !pathHasPrefix(path, prefix) {
				break
			}
			var state FileState
			if err := decodeFileState(value, &state); err != nil {
				return fmt.Errorf("decode snapshot %q: %w", path, err)
			}
			if err := fn(state); err != nil {
				return err
			}
		}
		return nil
	})
}

// Close flushes and closes the database. It is safe to call more than once.
func (s *BoltStore) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	s.closed = true
	return s.db.Close()
}

func (s *BoltStore) database() (*bolt.DB, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed {
		return nil, errors.New("fsrecon: bolt store is closed")
	}
	return s.db, nil
}

func (s *BoltStore) replaceSnapshot(ctx context.Context, source *bolt.DB, bucketName []byte) error {
	db, err := s.database()
	if err != nil {
		return err
	}
	return source.View(func(sourceTx *bolt.Tx) error {
		sourceBucket := sourceTx.Bucket(bucketName)
		return db.Update(func(tx *bolt.Tx) error {
			if err := tx.DeleteBucket(snapshotBucket); err != nil && !errors.Is(err, bolt.ErrBucketNotFound) {
				return err
			}
			destination, err := tx.CreateBucket(snapshotBucket)
			if err != nil {
				return err
			}
			cursor := sourceBucket.Cursor()
			for key, value := cursor.First(); key != nil; key, value = cursor.Next() {
				if err := ctx.Err(); err != nil {
					return err
				}
				if err := destination.Put(key, value); err != nil {
					return err
				}
			}
			return nil
		})
	})
}

type storedFileState struct {
	Path    string    `json:"path"`
	ID      string    `json:"id,omitempty"`
	Type    FileType  `json:"type"`
	Size    int64     `json:"size"`
	ModTime time.Time `json:"mod_time"`
	Mode    uint32    `json:"mode"`
	Schema  uint8     `json:"schema"`
}

func encodeFileState(state FileState) ([]byte, error) {
	return json.Marshal(storedFileState{
		Path: state.Path, ID: state.ID.value, Type: state.Type, Size: state.Size,
		ModTime: state.ModTime, Mode: uint32(state.Mode), Schema: 1,
	})
}

func decodeFileState(data []byte, state *FileState) error {
	var stored storedFileState
	if err := json.Unmarshal(data, &stored); err != nil {
		return err
	}
	if stored.Schema != 1 {
		return fmt.Errorf("unsupported snapshot schema %d", stored.Schema)
	}
	*state = FileState{
		Path: stored.Path, ID: newFileID(stored.ID), Type: stored.Type, Size: stored.Size,
		ModTime: stored.ModTime, Mode: fs.FileMode(stored.Mode),
	}
	return nil
}
