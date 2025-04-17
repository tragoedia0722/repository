package main

import (
	"context"
	"errors"
	"fmt"
	"github.com/ipfs/boxo/blockservice"
	"github.com/ipfs/boxo/blockstore"
	"github.com/ipfs/boxo/files"
	"github.com/ipfs/boxo/ipld/merkledag"
	unixfile "github.com/ipfs/boxo/ipld/unixfs/file"
	"github.com/ipfs/go-cid"
	"io"
	"os"
	"path/filepath"
	"strings"
)

const invalidChars = `<>:"/\|?*` + "\x00"

var (
	ErrInvalidDirectoryEntry = errors.New("invalid directory entry name")
	ErrPathExistsOverwrite   = errors.New("path already exists and overwriting is not allowed")
)

type Extractor struct {
	blockStore blockstore.Blockstore
}

func NewExtractor(blockStore blockstore.Blockstore) *Extractor {
	return &Extractor{
		blockStore: blockStore,
	}
}

func (e *Extractor) Extract(ctx context.Context, rootCid string, path string, overwrite bool) error {
	bs := blockservice.New(e.blockStore, nil)
	ds := merkledag.NewDAGService(bs)

	c, err := cid.Parse(rootCid)
	if err != nil {
		return err
	}

	node, err := ds.Get(ctx, c)
	if err != nil {
		return err
	}

	fileNode, err := unixfile.NewUnixfsFile(ctx, ds, node)
	if err != nil {
		return err
	}

	return e.writeTo(fileNode, path, overwrite)
}

func (e *Extractor) writeTo(nd files.Node, path string, allowOverwrite bool) error {
	if _, err := os.Lstat(path); err == nil {
		if !allowOverwrite {
			return ErrPathExistsOverwrite
		}

		fi, e1 := os.Stat(path)
		if e1 != nil {
			return e1
		}

		if !(fi.IsDir() && e.isDir(nd)) {
			if err = os.RemoveAll(path); err != nil {
				return fmt.Errorf("failed to remove existing path: %w", err)
			}
		}
	} else if !os.IsNotExist(err) {
		return err
	}

	switch node := nd.(type) {
	case *files.Symlink:
		return os.Symlink(node.Target, path)
	case files.File:
		f, err := e.createNewFile(path)
		if err != nil {
			return err
		}
		defer func() {
			_ = f.Close()
		}()
		_, err = io.Copy(f, node)
		if err != nil {
			return err
		}
		return nil
	case files.Directory:
		if _, err := os.Stat(path); os.IsNotExist(err) {
			if err = os.Mkdir(path, 0o777); err != nil {
				return err
			}
		}

		entries := node.Entries()
		for entries.Next() {
			entryName := entries.Name()
			if entryName == "" ||
				entryName == "." ||
				entryName == ".." ||
				!e.isValidFilename(entryName) {
				return ErrInvalidDirectoryEntry
			}
			child := filepath.Join(path, entryName)
			if err := e.writeTo(entries.Node(), child, allowOverwrite); err != nil {
				return err
			}
		}
		return entries.Err()
	default:
		return fmt.Errorf("file type %T at %q is not supported", node, path)
	}
}

func (*Extractor) createNewFile(path string) (*os.File, error) {
	return os.OpenFile(path, os.O_EXCL|os.O_CREATE|os.O_WRONLY, 0o666)
}

func (*Extractor) isValidFilename(filename string) bool {
	return !strings.ContainsAny(filename, invalidChars)
}

func (*Extractor) isDir(nd files.Node) bool {
	_, isDir := nd.(files.Directory)
	return isDir
}
