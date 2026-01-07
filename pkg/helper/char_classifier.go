package helper

import (
	"unicode"
)

// 无效字符查找表 - 包初始化时自动构建
var invalidCharTable [256]bool

// 保留名集合 - O(1) 查找
var reservedNameSet = map[string]bool{
	"con": true, "prn": true, "aux": true, "nul": true,
	"com1": true, "com2": true, "com3": true, "com4": true,
	"com5": true, "com6": true, "com7": true, "com8": true,
	"com9": true,
	"lpt1": true, "lpt2": true, "lpt3": true, "lpt4": true,
	"lpt5": true, "lpt6": true, "lpt7": true, "lpt8": true,
	"lpt9": true,
}

func init() {
	// 初始化无效字符查找表
	for _, c := range invalidChars {
		if c < 256 {
			invalidCharTable[c] = true
		}
	}
}

// classifyCharacter 分类字符并返回应该执行的操作
func classifyCharacter(r rune) charAction {
	// 快速路径：ASCII 无效字符
	if r < 256 && invalidCharTable[r] {
		return actionReplaceWithUnderscore
	}

	// 检查是否应该移除
	if shouldRemove(r) {
		return actionRemove
	}

	// 检查是否应该替换为空格
	if shouldReplaceWithSpace(r) {
		return actionReplaceWithSpace
	}

	// 控制字符（除了 Tab）
	if r >= 0 && r <= 31 && r != '\t' {
		return actionRemove
	}

	// 非打印字符（包括 Tab、换行符等）
	if !unicode.IsPrint(r) {
		return actionRemove
	}

	return actionKeep
}

// shouldRemove 判断字符是否应该被移除
// 这些字符在文件名中不应该出现，且没有合适的替换
func shouldRemove(r rune) bool {
	// LTR/RTL 标记
	if r == runeLTRMark || r == runeRTLMark {
		return true
	}

	// 双向嵌入字符
	if r >= runeLRE && r <= runeRLO {
		return true
	}

	// 零宽字符
	if r >= runeZeroWidthSpace && r <= runeZeroWidthJoiner {
		return true
	}

	// 格式字符
	if r == runeWordJoiner || r == runeBOM {
		return true
	}

	// 其他控制字符
	switch r {
	case runeSoftHyphen, runeCombiningGraphemeJoiner,
		runeArabicLetterMark, runeMongolianVowelSeparator:
		return true
	}

	// DEL 字符
	if r == runeDEL {
		return true
	}

	return false
}

// shouldReplaceWithSpace 判断字符是否应该替换为空格
// 这些是各种 Unicode 空格字符，应该标准化为普通空格
func shouldReplaceWithSpace(r rune) bool {
	// Non-breaking spaces and separators
	switch r {
	case runeNonBreakingSpace, runeIdeographicSpace,
		runeLineSeparator, runeParagraphSeparator:
		return true
	}

	// Various space characters (U+2000 to U+200A)
	if r >= runeEnQuad && r <= runeHairSpace {
		return true
	}

	// Narrow no-break space
	if r == runeNarrowNoBreakSpace {
		return true
	}

	// Medium mathematical space
	if r == runeMediumMathematicalSpace {
		return true
	}

	return false
}

// ReservedNames 返回所有 Windows 保留的设备名列表
// 这些名称在 Windows 中有特殊含义，不能用作文件名
func ReservedNames() []string {
	return []string{
		"CON", "PRN", "AUX", "NUL",
		"COM1", "COM2", "COM3", "COM4", "COM5",
		"COM6", "COM7", "COM8", "COM9",
		"LPT1", "LPT2", "LPT3", "LPT4", "LPT5",
		"LPT6", "LPT7", "LPT8", "LPT9",
	}
}
