package repo

import (
	"bytes"
	"encoding/json"
)

type DiskSpec map[string]interface{}

func DefaultDiskSpec() DiskSpec {
	return map[string]interface{}{
		"type": "mount",
		"mounts": []interface{}{
			map[string]interface{}{
				"mountpoint": "/blocks",
				"type":       "measure",
				"prefix":     "flatfs.datastore",
				"child": map[string]interface{}{
					"type":      "flatfs",
					"path":      "blocks",
					"sync":      true,
					"shardFunc": "/repo/flatfs/shard/v1/next-to-last/2",
				},
			},
			map[string]interface{}{
				"mountpoint": "/",
				"type":       "measure",
				"prefix":     "leveldb.datastore",
				"child": map[string]interface{}{
					"type":        "levelds",
					"path":        "datastore",
					"compression": "none",
				},
			},
		},
	}
}

func (s DiskSpec) Bytes() []byte {
	b, err := json.Marshal(s)
	if err != nil {
		panic(err)
	}

	return bytes.TrimSpace(b)
}

func (s DiskSpec) String() string {
	return string(s.Bytes())
}
