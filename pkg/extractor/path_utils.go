package extractor

import (
	"fmt"
	"path/filepath"
	"strings"

	"github.com/tragoedia0722/repository/pkg/helper"
)

// cleanPathComponents cleans each component of a path and returns the cleaned path.
// Empty components are preserved. Components that clean to empty strings are replaced
// with the default cleaned directory name.
func cleanPathComponents(path string) string {
	cleanPath := filepath.Clean(path)

	pathParts := strings.Split(cleanPath, string(filepath.Separator))
	cleanedParts := make([]string, 0, len(pathParts))

	for _, part := range pathParts {
		if part == "" {
			cleanedParts = append(cleanedParts, part)
			continue
		}

		cleanPart := helper.CleanFilename(part)
		if cleanPart == "" {
			cleanPart = defaultCleanedDirName
		}
		cleanedParts = append(cleanedParts, cleanPart)
	}

	return strings.Join(cleanedParts, string(filepath.Separator))
}

// normalizeEntryName normalizes a directory entry name, handling both simple names
// and paths containing backslashes. Returns the cleaned name and an error if the
// name is invalid.
func normalizeEntryName(entryName string) (string, error) {
	// Handle backslash-separated paths (Windows-style)
	if strings.Contains(entryName, "\\") {
		return normalizeBackslashPath(entryName)
	}

	// Handle simple names
	cleanName := helper.CleanFilename(entryName)
	if cleanName == "" {
		return "", wrapInvalidDirectoryEntry(entryName)
	}

	return cleanName, nil
}

// normalizeBackslashPath normalizes a path containing backslashes by converting
// them to forward slashes, then validates and cleans each component.
// Returns the normalized path and an error if any component is invalid.
func normalizeBackslashPath(entryName string) (string, error) {
	normalizedPath := strings.ReplaceAll(entryName, "\\", "/")
	pathParts := strings.Split(normalizedPath, "/")

	cleanedParts := make([]string, 0, len(pathParts))
	for _, part := range pathParts {
		// Skip empty parts and current directory references
		if part == "" || part == "." {
			continue
		}

		// Detect and prevent path traversal attempts
		if part == ".." {
			return "", wrapPathTraversalAttempt(entryName)
		}

		// Clean each component
		cleanPart := helper.CleanFilename(part)
		if cleanPart == "" {
			return "", wrapPathComponent(part)
		}
		cleanedParts = append(cleanedParts, cleanPart)
	}

	// Ensure at least one valid component
	if len(cleanedParts) == 0 {
		return "", fmt.Errorf("no valid path components: %s", entryName)
	}

	cleanedPath := strings.Join(cleanedParts, string(filepath.Separator))
	return cleanedPath, nil
}

// validateSymlinkTarget checks if a symlink target is valid.
// Valid targets must be relative paths that don't escape the current directory.
func validateSymlinkTarget(target string) bool {
	targetPath := filepath.Clean(target)

	// Reject absolute paths
	if filepath.IsAbs(targetPath) {
		return false
	}

	// Use filepath.Rel to detect path traversal attempts
	// This handles cases like:
	// - "./../../etc/passwd" -> will be detected as escaping
	// - "dir/../../../etc/passwd" -> will be detected as escaping
	rel, err := filepath.Rel(".", targetPath)
	if err != nil {
		return false
	}

	// Check if the relative path tries to escape the current directory
	// (e.g., "../file", "../../etc/passwd", "dir/../../../etc/passwd")
	if strings.HasPrefix(rel, ".."+string(filepath.Separator)) || rel == ".." {
		return false
	}

	return true
}
