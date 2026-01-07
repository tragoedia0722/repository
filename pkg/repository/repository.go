package repository

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/ipfs/boxo/blockstore"
	blocks "github.com/ipfs/go-block-format"
	cid2 "github.com/ipfs/go-cid"
	ipld "github.com/ipfs/go-ipld-format"
	"github.com/multiformats/go-multicodec"
	mh "github.com/multiformats/go-multihash"
	"github.com/tragoedia0722/repository/internal/storage"
	"golang.org/x/sync/errgroup"
)

const (
	// 默认重试配置
	defaultMaxRetries = 3
	defaultBaseDelay  = 50 * time.Millisecond

	// 默认目录权限
	defaultDirPerm = 0o750 // rwxr-x---

	// 默认并发限制
	defaultMaxConcurrency = 100

	// 数据块大小限制
	maxBlockSize = 128 * 1024 * 1024 // 128MB
)

// Repository 表示一个 IPFS 风格的内容寻址存储仓库。
//
// Repository 提供了基于 CID（Content Identifier）的内容存储和检索功能，
// 使用内容寻址的方式确保数据的完整性和可验证性。
type Repository struct {
	storage    *storage.Storage
	blockStore blockstore.Blockstore
	builder    cid2.Builder
}

// NewRepository 创建或打开一个仓库实例。
//
// 如果仓库目录不存在，会自动创建。目录权限设置为 0o750（rwxr-x---）。
//
// 参数：
//
//	path - 仓库路径
//
// 返回：
//
//	*Repository - 仓库实例
//	error - 如果创建失败，返回错误
func NewRepository(path string) (*Repository, error) {
	// 验证路径不为空
	if path == "" {
		return nil, fmt.Errorf("repository path cannot be empty")
	}

	// 清理路径
	path = filepath.Clean(path)

	// 使用安全的默认权限创建目录
	if err := os.MkdirAll(path, defaultDirPerm); err != nil {
		return nil, fmt.Errorf("failed to create repository directory: %w", err)
	}

	s, err := storage.NewStorage(path)
	if err != nil {
		return nil, fmt.Errorf("failed to create storage: %w", err)
	}

	return &Repository{
		storage:    s,
		blockStore: blockstore.NewBlockstore(s.Datastore()),
		builder: cid2.V1Builder{
			Codec:    uint64(multicodec.DagPb),
			MhType:   mh.SHA2_256,
			MhLength: -1,
		},
	}, nil
}

// BlockStore 返回底层 blockstore。
// 注意：调用者不应该关闭或修改返回的 blockstore。
func (r *Repository) BlockStore() blockstore.Blockstore {
	return r.blockStore
}

// DataStore 返回底层数据存储。
func (r *Repository) DataStore() storage.Datastore {
	return r.storage.Datastore()
}

// Usage 返回存储使用情况（字节数）。
func (r *Repository) Usage(ctx context.Context) (uint64, error) {
	return r.storage.GetStorageUsage(ctx)
}

// Close 关闭仓库并释放资源。
//
// Close 是幂等的，多次调用不会返回错误。
func (r *Repository) Close() error {
	if r.storage == nil {
		return nil
	}
	return r.storage.Close()
}

// Destroy 销毁仓库并删除所有数据。
//
// 此操作不可逆，请谨慎使用。
// Destroy 是幂等的，多次调用不会返回错误。
func (r *Repository) Destroy() error {
	if r.storage == nil {
		return nil
	}
	return r.storage.Destroy()
}

// PutBlock 存储单个数据块并返回其 CID。
//
// 参数：
//
//	ctx - 用于取消操作的上下文
//	bytes - 要存储的数据
//
// 返回：
//
//	*cid2.Cid - 数据块的 CID
//	error - 如果存储失败，返回错误
func (r *Repository) PutBlock(ctx context.Context, bytes []byte) (*cid2.Cid, error) {
	// 验证数据大小
	if len(bytes) > maxBlockSize {
		return nil, fmt.Errorf("block size %d bytes exceeds maximum %d bytes", len(bytes), maxBlockSize)
	}

	sum, err := r.builder.Sum(bytes)
	if err != nil {
		return nil, fmt.Errorf("failed to calculate CID: %w", err)
	}

	blk, err := blocks.NewBlockWithCid(bytes, sum)
	if err != nil {
		return nil, fmt.Errorf("failed to create block: %w", err)
	}

	if err := r.blockStore.Put(ctx, blk); err != nil {
		return nil, fmt.Errorf("failed to put block: %w", err)
	}

	return &sum, nil
}

// PutBlockWithCid 使用指定 CID 存储数据块。
//
// 参数：
//
//	ctx - 用于取消操作的上下文
//	cid - CID 字符串
//	bytes - 要存储的数据
//
// 返回：
//
//	error - 如果存储失败，返回错误
func (r *Repository) PutBlockWithCid(ctx context.Context, cid string, bytes []byte) error {
	// 验证数据大小
	if len(bytes) > maxBlockSize {
		return fmt.Errorf("block size %d bytes exceeds maximum %d bytes", len(bytes), maxBlockSize)
	}

	c, err := r.parseCID(cid)
	if err != nil {
		return err
	}

	blk, err := blocks.NewBlockWithCid(bytes, c)
	if err != nil {
		return fmt.Errorf("failed to create block: %w", err)
	}

	if err := r.blockStore.Put(ctx, blk); err != nil {
		return fmt.Errorf("failed to put block: %w", err)
	}

	return nil
}

// PutManyBlocks 批量存储数据块并返回对应的 CID 列表。
//
// 参数：
//
//	ctx - 用于取消操作的上下文
//	bytes - 数据块列表
//
// 返回：
//
//	[]*cid2.Cid - CID 列表
//	error - 如果存储失败，返回错误
func (r *Repository) PutManyBlocks(ctx context.Context, bytes [][]byte) ([]*cid2.Cid, error) {
	if len(bytes) == 0 {
		return nil, nil
	}

	// 预分配固定长度 slice，避免 append 开销
	blks := make([]blocks.Block, len(bytes))
	cids := make([]*cid2.Cid, len(bytes))

	for i, b := range bytes {
		// 定期检查 context
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		// 验证数据大小
		if len(b) > maxBlockSize {
			return nil, fmt.Errorf("block at index %d: size %d bytes exceeds maximum %d bytes",
				i, len(b), maxBlockSize)
		}

		sum, err := r.builder.Sum(b)
		if err != nil {
			return nil, fmt.Errorf("failed to calculate CID at index %d: %w", i, err)
		}
		cids[i] = &sum

		blk, err := blocks.NewBlockWithCid(b, sum)
		if err != nil {
			return nil, fmt.Errorf("failed to create block at index %d: %w", i, err)
		}
		blks[i] = blk
	}

	if err := r.blockStore.PutMany(ctx, blks); err != nil {
		return nil, fmt.Errorf("failed to put blocks: %w", err)
	}

	return cids, nil
}

// HasBlock 检查指定 CID 的块是否存在。
//
// 参数：
//
//	ctx - 用于取消操作的上下文
//	cid - CID 字符串
//
// 返回：
//
//	bool - 如果块存在返回 true，否则返回 false
//	error - 如果检查失败，返回错误
func (r *Repository) HasBlock(ctx context.Context, cid string) (bool, error) {
	c, err := r.parseCID(cid)
	if err != nil {
		return false, err
	}

	return r.blockStore.Has(ctx, c)
}

// HasAllBlocks 检查所有指定的 CID 是否都存在。
//
// 使用并发检查以提高性能，最多同时运行 100 个 goroutine。
// 每个 goroutine 都有 panic 恢复机制，防止单个失败导致整个程序崩溃。
//
// 参数：
//
//	ctx - 用于取消操作的上下文
//	cids - CID 字符串列表
//
// 返回：
//
//	bool - 如果所有块都存在返回 true，否则返回 false
//	error - 如果检查失败，返回错误
func (r *Repository) HasAllBlocks(ctx context.Context, cids []string) (bool, error) {
	if len(cids) == 0 {
		return true, nil
	}

	g, ctx := errgroup.WithContext(ctx)
	g.SetLimit(defaultMaxConcurrency) // 限制并发数
	results := make([]bool, len(cids))

	for i, cidStr := range cids {
		i, cidStr := i, cidStr // 避免闭包问题
		g.Go(func() (err error) {
			// 添加 panic 恢复机制
			defer func() {
				if r := recover(); r != nil {
					err = fmt.Errorf("panic checking block %s: %v", cidStr, r)
				}
			}()

			c, err := r.parseCID(cidStr)
			if err != nil {
				return err
			}

			has, err := r.blockStore.Has(ctx, c)
			if err != nil {
				return fmt.Errorf("failed to check block %s: %w", cidStr, err)
			}

			results[i] = has
			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return false, err
	}

	// 检查是否全部存在
	for _, has := range results {
		if !has {
			return false, nil
		}
	}

	return true, nil
}

// GetRawData 获取指定 CID 的原始数据，支持指数退避重试。
//
// 使用指数退避策略（50ms → 100ms → 200ms）以提高响应速度。
// 最坏情况下延迟 350ms，而非原来的 1500ms。
//
// 参数：
//
//	ctx - 用于取消操作的上下文
//	cid - CID 字符串
//
// 返回：
//
//	[]byte - 原始数据
//	error - 如果获取失败，返回错误
func (r *Repository) GetRawData(ctx context.Context, cid string) ([]byte, error) {
	c, err := r.parseCID(cid)
	if err != nil {
		return nil, err
	}

	var lastErr error
	for retry := 0; retry < defaultMaxRetries; retry++ {
		blk, err := r.blockStore.Get(ctx, c)
		if err == nil {
			return blk.RawData(), nil
		}

		lastErr = err
		if !ipld.IsNotFound(err) {
			return nil, fmt.Errorf("failed to get block: %w", err)
		}

		// 如果不是最后一次重试，使用指数退避
		if retry < defaultMaxRetries-1 {
			delay := defaultBaseDelay * time.Duration(1<<uint(retry))
			select {
			case <-time.After(delay):
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
	}

	// 根据最后错误类型返回更准确的消息
	if ipld.IsNotFound(lastErr) {
		return nil, fmt.Errorf("block not found after %d retries", defaultMaxRetries)
	}
	return nil, fmt.Errorf("failed to get block after %d retries: %w", defaultMaxRetries, lastErr)
}

// DelBlock 删除指定 CID 的块。
//
// 参数：
//
//	ctx - 用于取消操作的上下文
//	cid - CID 字符串
//
// 返回：
//
//	error - 如果删除失败，返回错误
func (r *Repository) DelBlock(ctx context.Context, cid string) error {
	c, err := r.parseCID(cid)
	if err != nil {
		return err
	}

	if err := r.blockStore.DeleteBlock(ctx, c); err != nil {
		return fmt.Errorf("failed to delete block: %w", err)
	}

	return nil
}

// parseCID 解析 CID 字符串为 CID 对象。
//
// 这是一个辅助方法，用于统一 CID 解析逻辑和错误处理。
//
// 参数：
//
//	cidStr - CID 字符串
//
// 返回：
//
//	cid2.Cid - 解析后的 CID 对象
//	error - 如果解析失败，返回错误
func (r *Repository) parseCID(cidStr string) (cid2.Cid, error) {
	c, err := cid2.Parse(cidStr)
	if err != nil {
		return cid2.Cid{}, fmt.Errorf("invalid CID %q: %w", cidStr, err)
	}
	return c, nil
}
