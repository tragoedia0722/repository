package main

import (
	"context"
	"fmt"
	"git.hificloud.net/nas2024/cloud/demeter/repo/pkg/storage"
)

func main() {
	r, err := storage.NewStorage("./test_repo")
	if err != nil {
		panic(err)
	}

	usage, err := r.GetStorageUsage(context.Background())
	if err != nil {
		panic(err)
	}

	fmt.Println(usage)

	r.Close()
}
