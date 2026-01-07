package storage

import (
	"fmt"
	"os"
	"path/filepath"
)

// Writable 检查目录是否可写，并在必要时创建它。
//
// 参数：
//
//	path - 要检查的目录路径
//
// 返回：
//
//	error - 如果目录不可写或无法创建，返回包装后的错误
//
// 如果目录不存在，会尝试创建它。
// 通过创建临时文件来验证可写性。
func Writable(path string) error {
	// 使用 0755 而不是 os.ModePerm，避免权限过于宽松
	if err := os.MkdirAll(path, 0o755); err != nil {
		return &StorageError{
			Operation: "create directory",
			Path:      path,
			Err:       err,
		}
	}

	testFile := filepath.Join(path, "._check_writable")
	f, err := os.Create(testFile)
	if err != nil {
		return &StorageError{
			Operation: "check writability",
			Path:      path,
			Err:       fmt.Errorf("cannot create test file: %w", err),
		}
	}

	// 使用 defer 确保清理，即使发生错误
	defer func() {
		f.Close()
		os.Remove(testFile) // 尽力清理，忽略错误
	}()

	// 验证文件确实可写（通过同步）
	if err := f.Sync(); err != nil {
		return &StorageError{
			Operation: "check writability",
			Path:      path,
			Err:       fmt.Errorf("cannot sync test file: %w", err),
		}
	}

	return nil
}

// DatastoreSpecPath 返回 datastore 配置文件的路径。
//
// 配置文件名为 "datastore_spec"，位于给定的仓库路径中。
//
// 参数：
//
//	repoPath - 仓库根目录
//
// 返回：
//
//	string - datastore_spec 文件的完整路径
func DatastoreSpecPath(repoPath string) string {
	return filepath.Join(repoPath, "datastore_spec")
}

// FileExists 检查文件是否存在且非空。
//
// 参数：
//
//	filename - 要检查的文件路径
//
// 返回：
//
//	bool - 如果文件存在且大小 > 0，返回 true
//
// 注意：大小为 0 的文件会被视为不存在。
func FileExists(filename string) bool {
	fi, err := os.Stat(filename)
	if err != nil {
		return false
	}

	return fi.Size() > 0
}

// resolvePath 解析路径。
//
// 如果 basePath 是绝对路径，直接返回 basePath。
// 否则，将 basePath 连接到 rootPath 并返回结果。
//
// 参数：
//
//	rootPath - 根目录路径
//	basePath - 要解析的基准路径
//
// 返回：
//
//	string - 解析后的完整路径
//
// 示例：
//
//	resolvePath("/home/user", "data") → "/home/user/data"
//	resolvePath("/home/user", "/opt/data") → "/opt/data"
func resolvePath(rootPath, basePath string) string {
	if filepath.IsAbs(basePath) {
		return basePath
	}
	return filepath.Join(rootPath, basePath)
}
