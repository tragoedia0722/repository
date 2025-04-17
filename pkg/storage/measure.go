package storage

import (
	"fmt"
	"github.com/ipfs/go-ds-measure"
)

type measureDatastoreConfig struct {
	child  DatastoreConfig
	prefix string
}

func MeasureDatastoreConfig(params map[string]interface{}) (DatastoreConfig, error) {
	childField, ok := params["child"].(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("'child' field is missing or not a map")
	}

	child, err := AnyDatastoreConfig(childField)
	if err != nil {
		return nil, err
	}

	prefix, ok := params["prefix"].(string)
	if !ok {
		return nil, fmt.Errorf("'prefix' field was missing or not a string")
	}

	return &measureDatastoreConfig{child, prefix}, nil
}

func (c *measureDatastoreConfig) DiskSpec() DiskSpec {
	return c.child.DiskSpec()
}

func (c *measureDatastoreConfig) Create(path string) (Datastore, error) {
	child, err := c.child.Create(path)
	if err != nil {
		return nil, err
	}

	return measure.New(c.prefix, child), nil
}
