package storage

import (
	"context"
	"errors"
	"fmt"
	ds "github.com/ipfs/go-datastore"
	measure "github.com/ipfs/go-ds-measure"
	lockfile "github.com/ipfs/go-fs-lock"
	"github.com/mitchellh/go-homedir"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

const LockFile = ".storage.lock"

type Storage struct {
	locker   sync.Mutex
	closed   bool
	path     string
	lockfile io.Closer
	ds       Datastore
}

func (r *Storage) Datastore() Datastore {
	r.locker.Lock()
	defer r.locker.Unlock()

	d := r.ds
	return d
}

func (r *Storage) GetStorageUsage(ctx context.Context) (uint64, error) {
	return ds.DiskUsage(ctx, r.Datastore())
}

func (r *Storage) Close() error {
	r.locker.Lock()
	defer r.locker.Unlock()

	if r.closed {
		return errors.New("storage is closed")
	}

	if err := r.ds.Close(); err != nil {
		return err
	}

	r.closed = true

	return r.lockfile.Close()
}

func NewStorage(path string) (*Storage, error) {
	if err := initSpec(path, DefaultDiskSpec()); err != nil {
		return nil, err
	}

	storage, err := open(path)
	if err != nil {
		return nil, err
	}

	return storage, nil
}

func initSpec(path string, conf map[string]interface{}) error {
	specPath := DatastoreSpecPath(path)
	if FileExists(specPath) {
		return nil
	}

	dsc, err := AnyDatastoreConfig(conf)
	if err != nil {
		return err
	}

	bytes := dsc.DiskSpec().Bytes()

	return os.WriteFile(specPath, bytes, 0o600)
}

func open(path string) (*Storage, error) {
	r, err := newStorage(path)
	if err != nil {
		return nil, err
	}

	r.locker.Lock()
	defer r.locker.Unlock()

	r.lockfile, err = lockfile.Lock(r.path, LockFile)
	if err != nil {
		return nil, err
	}

	keepLocked := false
	defer func() {
		if !keepLocked {
			_ = r.lockfile.Close()
		}
	}()

	if err = Writable(r.path); err != nil {
		return nil, err
	}

	if err = r.openDatastore(); err != nil {
		return nil, err
	}

	keepLocked = true

	return r, nil
}

func newStorage(path string) (*Storage, error) {
	if path == "" {
		return nil, errors.New("no path provided")
	}

	expPath, err := homedir.Expand(filepath.Clean(path))
	if err != nil {
		return nil, err
	}

	return &Storage{path: expPath}, nil
}

func (r *Storage) openDatastore() error {
	dsc, err := AnyDatastoreConfig(DefaultDiskSpec())
	if err != nil {
		return err
	}
	spec := dsc.DiskSpec()

	oldSpec, err := r.readSpec()
	if err != nil {
		return err
	}

	if oldSpec != spec.String() {
		return fmt.Errorf("datastore configuration of '%s' does not match what is on disk '%s'",
			oldSpec, spec.String())
	}

	d, err := dsc.Create(r.path)
	if err != nil {
		return err
	}

	r.ds = d

	prefix := "ipfs.storage.datastore"
	r.ds = measure.New(prefix, r.ds)

	return nil
}

func (r *Storage) readSpec() (string, error) {
	path := DatastoreSpecPath(r.path)

	b, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}

	return strings.TrimSpace(string(b)), nil
}
