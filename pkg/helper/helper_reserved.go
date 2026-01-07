package helper

import (
	"strings"
)

// HandleReservedNames 处理 Windows 保留的设备名
// Windows 保留以下设备名，不能用作文件名（不区分大小写）：
// CON, PRN, AUX, NUL, COM1-9, LPT1-9
//
// 如果文件名匹配保留名，会添加 "_file" 后缀。
// 例如: "CON.txt" -> "CON_file.txt", "con" -> "con_file"
func HandleReservedNames(filename string) string {
	if filename == "" {
		return ""
	}

	// 分离文件名和扩展名
	base, ext := splitNameAndExt(filename)

	// 检查是否是保留名
	if isReservedName(base) {
		return base + ReservedSuffix + ext
	}

	return filename
}

// splitNameAndExt 分离文件名和扩展名
// 返回 (baseName, extension)
//
// 例如:
//
//	"file.txt" -> ("file", ".txt")
//	"file" -> ("file", "")
//	".gitignore" -> ("", ".gitignore")
func splitNameAndExt(filename string) (string, string) {
	dotIndex := strings.LastIndex(filename, ".")

	// 没有点，点在开头，或点在末尾
	if dotIndex <= 0 || dotIndex >= len(filename)-1 {
		return filename, ""
	}

	return filename[:dotIndex], filename[dotIndex:]
}

// isReservedName 检查文件名是否是 Windows 保留的设备名
// 检查是不区分大小写的
func isReservedName(name string) bool {
	lower := strings.ToLower(name)
	return reservedNameSet[lower]
}
