package storage

import (
	"fmt"
	ds "github.com/ipfs/go-datastore"
)

var datastore map[string]ConfigFromMap

type Datastore interface {
	ds.Batching
}

type DatastoreConfig interface {
	DiskSpec() DiskSpec
	Create(path string) (Datastore, error)
}

type ConfigFromMap func(map[string]interface{}) (DatastoreConfig, error)

func init() {
	datastore = map[string]ConfigFromMap{
		"mount":   MountDatastoreConfig,
		"measure": MeasureDatastoreConfig,
		"levelds": LevelDBDatastoreConfig,
		"flatfs":  FlatFsDatastoreConfig,
	}
}

func AnyDatastoreConfig(params map[string]interface{}) (DatastoreConfig, error) {
	which, ok := params["type"].(string)
	if !ok {
		return nil, fmt.Errorf("'type' field missing or not a string")
	}

	fn, ok := datastore[which]
	if !ok {
		return nil, fmt.Errorf("unknown datastore type: %s", which)
	}

	return fn(params)
}
