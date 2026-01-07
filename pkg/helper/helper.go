package helper

import (
	"strings"
	"unicode/utf8"
)

// CleanFilename 清理文件名，使其适合在 Windows 文件系统中使用
//
// 本函数执行以下操作：
//  1. 移除无效字符 (<, >, :, ", /, \, |, ?, *, 等)
//  2. 移除控制字符和 Unicode 控制码
//  3. 标准化空格字符（合并连续空格）
//  4. 处理 Windows 保留的设备名（CON, PRN, AUX, 等）
//  5. 截断过长的文件名（255 字符）
//
// 参数：
//
//	filename - 要清理的文件名
//
// 返回：
//
//	清理后的文件名，如果输入为空返回 "unnamed_file"
//
// 示例：
//
//	CleanFilename("test<>:file.txt")    // "test___file.txt"
//	CleanFilename("CON.txt")            // "CON_file.txt"
//	CleanFilename("测试文件.txt")        // "测试文件.txt"
//	CleanFilename("file   name.txt")    // "file name.txt"
//	CleanFilename("")                    // "unnamed_file"
func CleanFilename(filename string) string {
	if filename == "" {
		return DefaultFilename
	}

	// 步骤 1: 清理字符（移除和替换）
	cleaned := cleanChars(filename)

	// 步骤 2: 标准化空格（合并连续空格，修剪首尾）
	cleaned = normalizeSpaces(cleaned)

	// 步骤 3: 修剪尾部空格和点（第二次修剪，确保干净）
	cleaned = strings.TrimRight(cleaned, ". ")

	// 步骤 4: 处理 Windows 保留名
	cleaned = HandleReservedNames(cleaned)

	// 步骤 5: 截断过长的文件名
	cleaned = TruncateFilename(cleaned, MaxFilenameLength)

	// 最终检查：如果结果为空，返回默认文件名
	if cleaned == "" {
		return DefaultFilename
	}

	return cleaned
}

// TruncateFilename 截断文件名到指定最大长度
// 如果文件名有扩展名，会尝试保留扩展名，只截断主文件名部分
//
// 参数：
//
//	filename - 要截断的文件名
//	maxLength - 最大长度（通常为 255）
//
// 返回：
//
//	截断后的文件名，如果不超过 maxLength 则原样返回
//
// 截断策略：
//   - 如果没有扩展名：直接截断到 maxLength
//   - 如果有扩展名：优先保留扩展名，截断主文件名部分
//   - 如果扩展名太长：无法保留完整扩展名时，截断整个文件名
//   - **重要**：始终在 UTF-8 字符边界处截断，不会产生无效的 UTF-8 序列
//
// 示例：
//
//	TruncateFilename("verylongfilename.txt", 15)  // "verylongfil.txt"
//	TruncateFilename("文件名称.txt", 8)            // "文件.txt" (不破坏 UTF-8)
//	TruncateFilename("normal.txt", 255)           // "normal.txt"
func TruncateFilename(filename string, maxLength int) string {
	// 如果文件名长度不超过最大长度，直接返回
	if len(filename) <= maxLength {
		return filename
	}

	// 查找最后一个点（扩展名分隔符）
	dotIndex := strings.LastIndex(filename, ".")

	// 没有点，点在开头，或点在末尾
	if dotIndex <= 0 || dotIndex == len(filename)-1 {
		// 安全截断整个文件名
		return safeTruncate(filename, maxLength)
	}

	// 分离主文件名和扩展名
	ext := filename[dotIndex:]  // 包含点的扩展名
	name := filename[:dotIndex] // 主文件名

	// 计算主文件名可用长度
	minNameLength := 1
	maxNameLength := maxLength - len(ext)

	// 如果扩展名太长，无法保留
	if maxNameLength < minNameLength {
		return safeTruncate(filename, maxLength)
	}

	// 安全截断主文件名，保留扩展名
	return safeTruncate(name, maxNameLength) + ext
}

// safeTruncate 安全地截断字符串到指定字节长度
// 确保不会在多字节 UTF-8 字符的中间进行切割
//
// 参数：
//
//	s - 要截断的字符串
//	maxLen - 最大字节长度
//
// 返回：
//
//	截断后的字符串，保证是有效的 UTF-8
//
// 如果无法在指定长度内找到有效的 UTF-8 边界，返回空字符串
func safeTruncate(s string, maxLen int) string {
	if len(s) <= maxLen {
		return s
	}

	// 从 maxLen 开始向前查找最后一个有效的 UTF-8 边界
	for i := maxLen; i > 0; i-- {
		if utf8.ValidString(s[:i]) {
			return s[:i]
		}
	}

	// 如果找不到有效边界（极少见），返回空字符串
	return ""
}
