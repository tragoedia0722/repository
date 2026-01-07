package validator

import (
	"context"
	"fmt"
	"sync"

	"github.com/ipfs/boxo/blockservice"
	"github.com/ipfs/boxo/blockstore"
	"github.com/ipfs/boxo/ipld/merkledag"
	"github.com/ipfs/go-cid"
	ipld "github.com/ipfs/go-ipld-format"
)

const (
	// defaultEstimatedIssues is the minimum estimated number of issues for slice pre-allocation
	defaultEstimatedIssues = 10

	// issuesEstimateRatio is the ratio used to estimate the number of issues
	issuesEstimateRatio = 10
)

// Validator validates IPFS DAGs and blocks.
//
// It checks for missing and invalid blocks, traverses DAGs to find required blocks,
// and provides detailed validation results including size calculations.
//
// Usage:
//
//	bs := blockstore.NewBlockstore() // your blockstore implementation
//	v := validator.NewValidator(bs)
//
//	rootCID := "Qm..."
//	blocks := []string{"Qm...", "Qm..."}
//
//	result, err := v.Validate(context.Background(), rootCID, blocks)
//	if err != nil {
//	    return fmt.Errorf("validation failed: %w", err)
//	}
//
//	if result.IsComplete {
//	    fmt.Println("All blocks present and valid")
//	} else {
//	    fmt.Printf("Missing %d blocks\n", len(result.MissingBlocks))
//	}
type Validator struct {
	blockStore blockstore.Blockstore
	dagService ipld.DAGService
}

// Result contains the validation results.
//
// It provides comprehensive information about the validation process including
// missing blocks, invalid blocks, error details, size calculations, and whether
// the data can be successfully restored.
//
// All methods that modify Result are thread-safe.
type Result struct {
	mu sync.Mutex

	// IsComplete indicates that all required blocks are present and valid
	IsComplete bool

	// MissingBlocks contains CIDs that were referenced but not found in the blockstore
	MissingBlocks []string

	// InvalidBlocks contains CIDs that could not be decoded or are invalid
	InvalidBlocks []string

	// ReachableSize is the total size of all blocks reachable from the root (in bytes)
	ReachableSize int64

	// CanRestore indicates whether the data can be fully restored from the available blocks
	CanRestore bool

	// ErrorDetails contains detailed error messages for any issues encountered during validation
	ErrorDetails []string
}

// NewValidator creates a new Validator with the given blockstore.
//
// The validator will use the provided blockstore to check for block existence
// and retrieve block data. A DAG service is automatically created for DAG traversal.
func NewValidator(blockStore blockstore.Blockstore) *Validator {
	bs := blockservice.New(blockStore, nil)
	return &Validator{
		blockStore: blockStore,
		dagService: merkledag.NewDAGService(bs),
	}
}

// Validate performs validation of the specified blocks and DAG.
//
// It validates the provided blocks list, checks for missing and invalid blocks,
// traverses the DAG from the root CID to find all required blocks, and provides
// detailed results about the validation process.
//
// Parameters:
//   - ctx: Context for cancellation and timeout
//   - rootCid: The root CID of the DAG to validate
//   - blocks: List of block CIDs to check (can be empty)
//
// Returns:
//   - *Result: Detailed validation results
//   - error: Any critical error that prevents validation (not validation failures themselves)
//
// Example:
//
//	result, err := v.Validate(ctx, "Qmabc...", []string{"Qmabc...", "Qmdef..."})
//	if err != nil {
//	    return err
//	}
//	if !result.IsComplete {
//	    fmt.Printf("Validation failed: %d missing blocks\n", len(result.MissingBlocks))
//	}
func (v *Validator) Validate(ctx context.Context, rootCid string, blocks []string) (*Result, error) {
	// Input validation
	if err := v.validateInputs(rootCid, blocks); err != nil {
		return nil, err
	}

	// Decode root CID
	theRootCid, err := cid.Decode(rootCid)
	if err != nil {
		return nil, fmt.Errorf("invalid root CID %q: %w", rootCid, err)
	}

	// Create result with pre-allocated capacity
	result := v.newResult(blocks)

	// Decode and validate all provided blocks
	blocksSet, err := v.validateBlocks(ctx, blocks, result)
	if err != nil {
		return nil, fmt.Errorf("block validation failed: %w", err)
	}

	// Traverse DAG to find required blocks
	requiredBlocks, reachableSize, err := v.findRequiredBlocks(ctx, theRootCid)
	if err != nil {
		result.addError("DAG traversal failed: %v", err)
		result.setCanRestore(false)
		return result, nil
	}

	result.ReachableSize = reachableSize

	// Check for missing required blocks
	v.checkMissingRequiredBlocks(blocksSet, requiredBlocks, result)

	// Finalize result
	result.finalize()

	return result, nil
}

// validateInputs validates the input parameters.
func (v *Validator) validateInputs(rootCid string, blocks []string) error {
	if rootCid == "" {
		return fmt.Errorf("root CID cannot be empty")
	}
	if blocks == nil {
		return fmt.Errorf("blocks list cannot be nil")
	}
	// Empty slice is allowed - we'll only validate the DAG structure
	return nil
}

// newResult creates a new Result with pre-allocated capacity based on the number of blocks.
func (v *Validator) newResult(blocks []string) *Result {
	estimatedIssues := len(blocks) / issuesEstimateRatio
	if estimatedIssues < defaultEstimatedIssues {
		estimatedIssues = defaultEstimatedIssues
	}

	return &Result{
		MissingBlocks: make([]string, 0, estimatedIssues),
		InvalidBlocks: make([]string, 0, estimatedIssues),
		ErrorDetails:  make([]string, 0, estimatedIssues),
	}
}

// decodedBlock represents a decoded CID with its validation status.
type decodedBlock struct {
	original string
	decoded  cid.Cid
	valid    bool
}

// validateBlocks decodes and validates all provided blocks.
// Returns a map of CID string to existence status.
func (v *Validator) validateBlocks(ctx context.Context, blocks []string, result *Result) (map[string]bool, error) {
	decoded := v.decodeCIDs(blocks)

	// Estimate valid blocks (assuming most will be valid)
	estimatedValid := len(blocks) * 3 / 4
	if estimatedValid < 10 {
		estimatedValid = 10
	}

	blocksSet := make(map[string]bool, estimatedValid)
	for _, db := range decoded {
		// Check for context cancellation
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		// Check block and populate result
		valid := v.checkBlock(ctx, db, result)
		if valid {
			blocksSet[db.original] = true
		}
	}

	return blocksSet, nil
}

// decodeCIDs decodes all CIDs and returns their validation status.
func (v *Validator) decodeCIDs(blocks []string) []decodedBlock {
	decoded := make([]decodedBlock, len(blocks))
	for i, cidStr := range blocks {
		c, err := cid.Decode(cidStr)
		decoded[i] = decodedBlock{
			original: cidStr,
			decoded:  c,
			valid:    err == nil,
		}
	}
	return decoded
}

// checkBlock validates a single block and updates the result accordingly.
// Returns true if the block is valid and exists, false otherwise.
func (v *Validator) checkBlock(ctx context.Context, db decodedBlock, result *Result) bool {
	// Check if CID is valid
	if !db.valid {
		result.addInvalidBlock(db.original)
		result.addError("invalid CID: %s", db.original)
		return false
	}

	// Check if block exists
	has, err := v.blockStore.Has(ctx, db.decoded)
	if err != nil {
		result.addError("error checking block %s: %v", db.original, err)
		return false
	}

	if !has {
		result.addMissingBlock(db.original)
		return false
	}

	return true
}

// findRequiredBlocks traverses the DAG and finds all required blocks.
// Returns a map of required CID strings and the total reachable size.
func (v *Validator) findRequiredBlocks(ctx context.Context, rootCid cid.Cid) (map[string]bool, int64, error) {
	requiredBlocks := make(map[string]bool)
	size, err := v.walkDAG(ctx, rootCid, requiredBlocks)
	return requiredBlocks, size, err
}

// walkDAG traverses the DAG from the root CID and calculates the total size.
//
// It visits each node in the DAG, adds it to the requiredBlocks map, and sums
// the size of all blocks. Uses concurrent traversal for performance.
// Thread-safe: protected by mutex for concurrent access.
func (v *Validator) walkDAG(ctx context.Context, rootCid cid.Cid, requiredBlocks map[string]bool) (int64, error) {
	var totalSize int64
	var sizeMutex sync.Mutex

	err := merkledag.Walk(ctx, merkledag.GetLinksWithDAG(v.dagService), rootCid, func(c cid.Cid) bool {
		cidStr := c.String()

		// Thread-safe map access
		sizeMutex.Lock()
		alreadyVisited := requiredBlocks[cidStr]
		if !alreadyVisited {
			requiredBlocks[cidStr] = true
		}
		sizeMutex.Unlock()

		if alreadyVisited {
			return false
		}

		// Get size efficiently (without loading entire block)
		if size, err := v.blockStore.GetSize(ctx, c); err == nil {
			sizeMutex.Lock()
			totalSize += int64(size)
			sizeMutex.Unlock()
		}

		return true
	}, merkledag.Concurrent())

	return totalSize, err
}

// checkMissingRequiredBlocks checks for required blocks that are missing from the provided blocks list.
func (v *Validator) checkMissingRequiredBlocks(blocksSet map[string]bool, requiredBlocks map[string]bool, result *Result) {
	for requiredCid := range requiredBlocks {
		if !blocksSet[requiredCid] {
			result.addMissingBlock(requiredCid)
		}
	}
}

// Result helper methods
// All methods are thread-safe.

// addMissingBlock adds a CID to the MissingBlocks list.
func (r *Result) addMissingBlock(cid string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.MissingBlocks = append(r.MissingBlocks, cid)
}

// addInvalidBlock adds a CID to the InvalidBlocks list.
func (r *Result) addInvalidBlock(cid string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.InvalidBlocks = append(r.InvalidBlocks, cid)
}

// addError adds a formatted error message to the ErrorDetails list.
func (r *Result) addError(format string, args ...interface{}) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.ErrorDetails = append(r.ErrorDetails, fmt.Sprintf(format, args...))
}

// setCanRestore sets the CanRestore flag.
func (r *Result) setCanRestore(canRestore bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.CanRestore = canRestore
}

// finalize finalizes the result by setting IsComplete and CanRestore flags.
func (r *Result) finalize() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.IsComplete = len(r.MissingBlocks) == 0 && len(r.InvalidBlocks) == 0
	if r.CanRestore { // Only set if not already set to false
		r.CanRestore = r.IsComplete
	}
}
