package importer

import (
	"path/filepath"

	"github.com/tragoedia0722/repository/pkg/helper"
)

// cleanFilename cleans a filename and provides a fallback if empty.
// It preserves the file extension if the cleaned filename is empty.
func cleanFilename(original string) string {
	cleaned := helper.CleanFilename(original)
	if cleaned != "" {
		return cleaned
	}

	ext := filepath.Ext(original)
	if ext != "" {
		return defaultFileName + ext
	}
	return defaultFileName
}

// cleanDirname cleans a directory name and provides a fallback if empty.
func cleanDirname(original string) string {
	cleaned := helper.CleanFilename(original)
	if cleaned != "" {
		return cleaned
	}
	return defaultDirName
}

// cleanEntryName cleans a directory entry name, handling both files and directories.
// If isDir is true, it uses the directory fallback; otherwise uses the file fallback.
func cleanEntryName(original string, isDir bool) string {
	cleaned := helper.CleanFilename(original)
	if cleaned != "" {
		return cleaned
	}

	if isDir {
		return defaultDirName
	}

	ext := filepath.Ext(original)
	if ext != "" {
		return defaultFileName + ext
	}
	return defaultFileName
}
