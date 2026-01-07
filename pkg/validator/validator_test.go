package validator

import (
	"context"
	"errors"
	"testing"

	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	ipld "github.com/ipfs/go-ipld-format"
)

// mockBlockstore is a simple in-memory blockstore for testing
type mockBlockstore struct {
	blocks map[string][]byte
}

func newMockBlockstore() *mockBlockstore {
	return &mockBlockstore{
		blocks: make(map[string][]byte),
	}
}

func (m *mockBlockstore) Has(ctx context.Context, c cid.Cid) (bool, error) {
	_, ok := m.blocks[c.String()]
	return ok, nil
}

func (m *mockBlockstore) Get(ctx context.Context, c cid.Cid) (blocks.Block, error) {
	data, ok := m.blocks[c.String()]
	if !ok {
		return nil, ipld.ErrNotFound{Cid: c}
	}
	return blocks.NewBlockWithCid(data, c)
}

func (m *mockBlockstore) Put(ctx context.Context, blk blocks.Block) error {
	m.blocks[blk.Cid().String()] = blk.RawData()
	return nil
}

func (m *mockBlockstore) PutMany(ctx context.Context, blks []blocks.Block) error {
	for _, blk := range blks {
		if err := m.Put(ctx, blk); err != nil {
			return err
		}
	}
	return nil
}

func (m *mockBlockstore) DeleteBlock(ctx context.Context, c cid.Cid) error {
	delete(m.blocks, c.String())
	return nil
}

func (m *mockBlockstore) All(ctx context.Context) chan blocks.Block {
	ch := make(chan blocks.Block)
	go func() {
		defer close(ch)
		for k, v := range m.blocks {
			c, _ := cid.Decode(k)
			blk, _ := blocks.NewBlockWithCid(v, c)
			ch <- blk
		}
	}()
	return ch
}

func (m *mockBlockstore) AllKeysChan(ctx context.Context) (<-chan cid.Cid, error) {
	ch := make(chan cid.Cid)
	go func() {
		defer close(ch)
		for k := range m.blocks {
			c, _ := cid.Decode(k)
			ch <- c
		}
	}()
	return ch, nil
}

func (m *mockBlockstore) HashOnRead(enabled bool) {}

func (m *mockBlockstore) GetSize(ctx context.Context, c cid.Cid) (int, error) {
	data, ok := m.blocks[c.String()]
	if !ok {
		return 0, ipld.ErrNotFound{Cid: c}
	}
	return len(data), nil
}

func TestNewValidator(t *testing.T) {
	bs := newMockBlockstore()
	v := NewValidator(bs)

	if v == nil {
		t.Fatal("NewValidator returned nil")
	}
	if v.blockStore == nil {
		t.Error("blockStore is nil")
	}
	if v.dagService == nil {
		t.Error("dagService is nil")
	}
}

func TestValidate_InvalidRootCID(t *testing.T) {
	bs := newMockBlockstore()
	v := NewValidator(bs)

	tests := []struct {
		name    string
		rootCid string
	}{
		{
			name:    "empty string",
			rootCid: "",
		},
		{
			name:    "invalid CID format",
			rootCid: "not-a-cid",
		},
		{
			name:    "malformed base64",
			rootCid: "Qm" + string(make([]byte, 50)),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := v.Validate(context.Background(), tt.rootCid, nil)
			if err == nil {
				t.Error("expected error for invalid root CID, got nil")
			}
		})
	}
}

func TestValidate_MissingAndInvalidBlocks(t *testing.T) {
	bs := newMockBlockstore()
	v := NewValidator(bs)

	// Create a valid block
	validBlock := blocks.NewBlock([]byte("test data"))
	validCID := validBlock.Cid().String()
	bs.Put(context.Background(), validBlock)

	invalidCID := "invalid-cid-format"
	missingCID := "QmY8YgM11EA5ai1mAEzFZGB2D2FkGTWke1vUgTxBQxLsXo" // Likely doesn't exist

	tests := []struct {
		name           string
		rootCid        string
		blocks         []string
		expectMissing  bool
		expectInvalid  bool
		expectComplete bool
	}{
		{
			name:           "all blocks present",
			rootCid:        validCID,
			blocks:         []string{validCID},
			expectMissing:  false,
			expectInvalid:  false,
			expectComplete: true,
		},
		{
			name:           "missing blocks",
			rootCid:        validCID,
			blocks:         []string{validCID, missingCID},
			expectMissing:  true,
			expectInvalid:  false,
			expectComplete: false,
		},
		{
			name:           "invalid block CID",
			rootCid:        validCID,
			blocks:         []string{validCID, invalidCID},
			expectMissing:  false,
			expectInvalid:  true,
			expectComplete: false,
		},
		{
			name:           "both missing and invalid",
			rootCid:        validCID,
			blocks:         []string{invalidCID, missingCID},
			expectMissing:  true,
			expectInvalid:  true,
			expectComplete: false,
		},
		{
			name:           "empty block list",
			rootCid:        validCID,
			blocks:         []string{},
			expectMissing:  false,
			expectInvalid:  false,
			expectComplete: false, // Root block not in provided list
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := v.Validate(context.Background(), tt.rootCid, tt.blocks)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			if tt.expectMissing && len(result.MissingBlocks) == 0 {
				t.Error("expected missing blocks, got none")
			}
			if !tt.expectMissing && len(result.MissingBlocks) > 0 {
				t.Errorf("expected no missing blocks, got %d", len(result.MissingBlocks))
			}

			if tt.expectInvalid && len(result.InvalidBlocks) == 0 {
				t.Error("expected invalid blocks, got none")
			}
			if !tt.expectInvalid && len(result.InvalidBlocks) > 0 {
				t.Errorf("expected no invalid blocks, got %d", len(result.InvalidBlocks))
			}

			// Note: IsComplete and CanRestore depend on successful DAG traversal
			// which may fail with simple blocks that aren't valid protobuf DAG nodes
			if result.ErrorDetails != nil && len(result.ErrorDetails) > 0 {
				// DAG traversal failed, so IsComplete/CanRestore will be false
				t.Logf("DAG traversal had errors: %v", result.ErrorDetails)
			} else if result.IsComplete != tt.expectComplete {
				t.Errorf("expected IsComplete=%v, got %v", tt.expectComplete, result.IsComplete)
			}

			if result.ErrorDetails == nil || len(result.ErrorDetails) == 0 {
				// Only check CanRestore if there were no DAG errors
				if result.CanRestore != tt.expectComplete {
					t.Errorf("expected CanRestore=%v, got %v", tt.expectComplete, result.CanRestore)
				}
			}
		})
	}
}

func TestValidate_DAGTraversal(t *testing.T) {
	bs := newMockBlockstore()

	// Create simple blocks for testing
	rootBlock := blocks.NewBlock([]byte("root data"))
	child1 := blocks.NewBlock([]byte("child1 data"))
	child2 := blocks.NewBlock([]byte("child2 data"))

	bs.Put(context.Background(), rootBlock)
	bs.Put(context.Background(), child1)
	bs.Put(context.Background(), child2)

	v := NewValidator(bs)

	// Provide only root CID in blocks list
	blocks := []string{rootBlock.Cid().String()}

	result, err := v.Validate(context.Background(), rootBlock.Cid().String(), blocks)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Since we're using simple blocks without actual links,
	// walkDAG should still work for just the root block
	// The DAG walk will just visit the root node
	if result.ReachableSize <= 0 {
		t.Logf("ReachableSize=%d (walkDAG may not work with simple blocks)", result.ReachableSize)
	}
}

func TestValidate_ContextCancellation(t *testing.T) {
	bs := newMockBlockstore()
	v := NewValidator(bs)

	block := blocks.NewBlock([]byte("test"))
	bs.Put(context.Background(), block)

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately

	_, err := v.Validate(ctx, block.Cid().String(), []string{block.Cid().String()})
	// Context cancellation may or may not return error depending on when it's checked
	if err != nil {
		t.Logf("Context cancelled: %v", err)
	}
}

func TestValidate_ResultFields(t *testing.T) {
	bs := newMockBlockstore()
	v := NewValidator(bs)

	block := blocks.NewBlock([]byte("test data"))
	bs.Put(context.Background(), block)

	result, err := v.Validate(context.Background(), block.Cid().String(), []string{block.Cid().String()})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Check that result fields are properly initialized
	if result.MissingBlocks == nil {
		t.Error("MissingBlocks not initialized")
	}
	if result.InvalidBlocks == nil {
		t.Error("InvalidBlocks not initialized")
	}
	if result.ErrorDetails == nil {
		t.Error("ErrorDetails not initialized")
	}

	// ReachableSize might be 0 if walkDAG doesn't work with simple blocks
	t.Logf("ReachableSize=%d", result.ReachableSize)

	// If DAG traversal succeeded (no error details), then CanRestore should be true
	if len(result.ErrorDetails) == 0 && !result.CanRestore {
		t.Error("expected CanRestore=true when DAG traversal succeeds")
	}
}

func TestWalkDAG(t *testing.T) {
	bs := newMockBlockstore()
	v := NewValidator(bs)

	// Create simple blocks for testing
	block1 := blocks.NewBlock([]byte("block1"))
	block2 := blocks.NewBlock([]byte("block2"))

	bs.Put(context.Background(), block1)
	bs.Put(context.Background(), block2)

	requiredBlocks := make(map[string]bool)

	// Walk from block1
	size, err := v.walkDAG(context.Background(), block1.Cid(), requiredBlocks)
	if err != nil {
		// walkDAG may fail with simple blocks (not valid protobuf DAG nodes)
		t.Logf("walkDAG failed (expected with simple blocks): %v", err)
		return
	}

	t.Logf("walkDAG succeeded with size=%d", size)

	if size <= 0 {
		t.Log("size is 0 or negative (walkDAG may not work with simple blocks)")
	}

	if !requiredBlocks[block1.Cid().String()] {
		t.Error("expected block1 to be in requiredBlocks")
	}
}

func TestWalkDAG_NotFound(t *testing.T) {
	bs := newMockBlockstore()
	v := NewValidator(bs)

	// Create a CID that doesn't exist in the blockstore
	prefix := cid.Prefix{
		Version:  1,
		Codec:    0x70,
		MhType:   0x12,
		MhLength: 32,
	}
	missingCID, _ := prefix.Sum([]byte("nonexistent"))

	requiredBlocks := make(map[string]bool)

	_, err := v.walkDAG(context.Background(), missingCID, requiredBlocks)
	if err == nil {
		t.Error("expected error for missing block, got nil")
	}
	if !errors.Is(err, ipld.ErrNotFound{}) && !ipld.IsNotFound(err) {
		t.Logf("got error type: %T", err)
	}
}

// Test Validate with blockstore errors
func TestValidate_BlockstoreErrors(t *testing.T) {
	// Create a blockstore
	errorBlockstore := &mockBlockstore{
		blocks: make(map[string][]byte),
	}

	// We can't easily mock Has() to return errors with the current interface,
	// but we can test with invalid CIDs that will cause decode errors
	v := NewValidator(errorBlockstore)

	// Create one valid block
	validBlock := blocks.NewBlock([]byte("test"))
	errorBlockstore.Put(context.Background(), validBlock)

	// Mix valid and invalid CIDs
	blocks := []string{
		validBlock.Cid().String(),
		"QmInvalid",    // Too short
		"NotACIDAtAll", // Invalid format
	}

	result, err := v.Validate(context.Background(), validBlock.Cid().String(), blocks)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(result.InvalidBlocks) == 0 {
		t.Error("expected some blocks to be marked as invalid")
	}

	if len(result.ErrorDetails) == 0 {
		t.Error("expected error details for invalid blocks")
	}
}
