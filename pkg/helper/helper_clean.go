package helper

import (
	"strings"
)

// cleanChars 清理文件名中的字符
// 它移除无效字符，替换特殊字符，并使用 strings.Builder 优化性能
func cleanChars(filename string) string {
	// 预分配空间，避免多次扩容
	// 使用 75% 的原始长度作为估算，因为很多字符会被移除或替换
	var builder strings.Builder
	estimatedLen := len(filename) * 3 / 4
	if estimatedLen < 16 {
		estimatedLen = 16 // 最小预分配，避免小文件名频繁扩容
	}
	builder.Grow(estimatedLen)

	// 状态机跟踪
	lastWasSpace := false
	pendingSpace := false

	for _, r := range filename {
		action := classifyCharacter(r)

		switch action {
		case actionRemove:
			// 跳过此字符
			continue

		case actionReplaceWithSpace:
			// 替换为空格
			if !lastWasSpace && builder.Len() > 0 {
				pendingSpace = true
			}
			lastWasSpace = true
			continue

		case actionReplaceWithUnderscore:
			// 替换为下划线
			if pendingSpace {
				builder.WriteByte(' ')
				pendingSpace = false
			}
			lastWasSpace = false
			builder.WriteByte('_')
			continue

		case actionKeep:
			// 保留原字符
			if pendingSpace {
				builder.WriteByte(' ')
				pendingSpace = false
			}
			lastWasSpace = false
			builder.WriteRune(r)
		}
	}

	return builder.String()
}

// normalizeSpaces 标准化文件名中的空格
// 它合并连续空格，并修剪首尾空格和点
func normalizeSpaces(s string) string {
	// 使用 Fields 分割（自动处理各种空白字符）
	// 然后用单个空格连接
	fields := strings.Fields(s)
	if len(fields) == 0 {
		return ""
	}

	// 用单个空格连接
	result := strings.Join(fields, " ")

	// 修剪尾部空格和点
	result = strings.TrimRight(result, ". ")

	return result
}
