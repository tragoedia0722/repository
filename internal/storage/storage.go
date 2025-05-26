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

	return r.ds
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
	appendErrors(&errs, r.closeLockFile())

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

	err := r.closeLockFile()
	if err != nil {
		return fmt.Errorf("failed to close lock file: %v", err)
	}

	return os.RemoveAll(r.path)
}

func NewStorage(path string) (*Storage, error) {
	if err := initSpec(path, DefaultDiskSpec()); err != nil {
		return nil, err
	}

	return open(path)
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

	lockFile, err := createLockFile(lockPath)
	if err != nil {
		return nil, err
	}
	r.lockFile = lockFile

	if err = Writable(r.path); err != nil {
		return nil, err
	}

	if err = r.openDatastore(); err != nil {
		return nil, err
	}

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

	r.ds = measure.New("ipfs.storage.datastore", d)
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

func createLockFile(lockPath string) (*lockedfile.File, error) {
	lockfile, err := lockedfile.Create(lockPath)
	if err != nil {
		if os.IsExist(err) {
			return nil, fmt.Errorf("lock file already exists: %v", err)
		}

		return nil, err
	}

	if err = os.WriteFile(lockPath, []byte(strconv.Itoa(os.Getpid())), 0644); err != nil {
		_ = lockfile.Close()
		_ = os.Remove(lockPath)
		return nil, err
	}

	return lockfile, nil
}

func (r *Storage) closeLockFile() error {
	if r.lockFile != nil {
		if err := r.lockFile.Close(); err != nil {
			return err
		}

		lockPath := r.lockFile.Name()
		if err := os.Remove(lockPath); err != nil && !os.IsNotExist(err) {
			return err
		}
	}
	return nil
}

func appendErrors(errs *[]error, newErr error) {
	if newErr != nil {
		*errs = append(*errs, newErr)
	}
}
