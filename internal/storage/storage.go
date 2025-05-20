package storage

import (
	"context"
	"errors"
	"fmt"
	ds "github.com/ipfs/go-datastore"
	measure "github.com/ipfs/go-ds-measure"
	"github.com/mitchellh/go-homedir"
	"github.com/rogpeppe/go-internal/lockedfile"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
)

const LockFile = ".storage.lock"

type Storage struct {
	locker   sync.Mutex
	closed   bool
	path     string
	lockFile *lockedfile.File
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
		return nil
	}

	var errs []error

	if err := r.ds.Close(); err != nil {
		errs = append(errs, fmt.Errorf("datastore close error: %v", err))
	}

	r.closed = true

	if r.lockFile != nil {
		if err := r.lockFile.Close(); err != nil {
			errs = append(errs, fmt.Errorf("lock file close error: %v", err))
		}

		lockPath := r.lockFile.Name()
		if err := os.Remove(lockPath); err != nil && !os.IsNotExist(err) {
			errs = append(errs, fmt.Errorf("remove lock file error: %v", err))
		}
	}

	if len(errs) > 0 {
		return fmt.Errorf("errors during close: %v", errs)
	}

	return nil
}

func (r *Storage) Destroy() error {
	r.locker.Lock()
	defer r.locker.Unlock()

	if r.closed {
		return os.RemoveAll(r.path)
	}

	if err := r.ds.Close(); err != nil {
		return err
	}

	r.closed = true

	if r.lockFile != nil {
		if err := r.lockFile.Close(); err != nil {
			return err
		}

		lockPath := r.lockFile.Name()
		if err := os.Remove(lockPath); err != nil && !os.IsNotExist(err) {
			return err
		}
	}

	return os.RemoveAll(r.path)
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

	lockPath := filepath.Join(r.path, LockFile)

	lockFile, err := func() (*lockedfile.File, error) {
		file, e1 := lockedfile.Create(lockPath)
		if e1 != nil {
			if os.IsExist(e1) {
				if err = os.Remove(lockPath); err != nil {
					return nil, fmt.Errorf("failed to remove existing lock file: %v", err)
				}

				return lockedfile.Create(lockPath)
			}
			return nil, e1
		}

		if err = os.WriteFile(lockPath, []byte(strconv.Itoa(os.Getpid())), 0644); err != nil {
			_ = file.Close()
			_ = os.Remove(lockPath)
			return nil, err
		}

		return file, nil
	}()

	if err != nil {
		return nil, fmt.Errorf("create lock file error: %v", err)
	}

	r.lockFile = lockFile

	shouldKeepLock := false
	defer func() {
		if !shouldKeepLock {
			_ = lockFile.Close()
			_ = os.Remove(lockPath)
		}
	}()

	if err = Writable(r.path); err != nil {
		return nil, err
	}

	if err = r.openDatastore(); err != nil {
		return nil, err
	}

	shouldKeepLock = true
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
