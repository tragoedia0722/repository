package helper

const (
	// MaxFilenameLength 是 Windows 文件名最大长度
	MaxFilenameLength = 255

	// DefaultFilename 是默认文件名
	DefaultFilename = "unnamed_file"

	// ReservedSuffix 是保留名后缀
	ReservedSuffix = "_file"
)

// Unicode 字符常量 - 命名以提高可读性
const (
	// LTR/RTL 标记
	runeLTRMark = 0x200E // Left-to-Right Mark
	runeRTLMark = 0x200F // Right-to-Left Mark

	// 双向嵌入字符
	runeLRE = 0x202A // Left-to-Right Embedding
	runeRLE = 0x202B // Right-to-Left Embedding
	runePDF = 0x202C // Pop Directional Format
	runeLRO = 0x202D // Left-to-Right Override
	runeRLO = 0x202E // Right-to-Left Override

	// 零宽字符
	runeZeroWidthSpace     = 0x200B // Zero Width Space
	runeZeroWidthNonJoiner = 0x200C // Zero Width Non-Joiner
	runeZeroWidthJoiner    = 0x200D // Zero Width Joiner

	// 格式字符
	runeWordJoiner = 0x2060 // Word Joiner
	runeBOM        = 0xFEFF // Byte Order Mark

	// 空格字符
	runeNonBreakingSpace        = 0x00A0 // Non-Breaking Space (NBSP)
	runeIdeographicSpace        = 0x3000 // Ideographic Space
	runeEnQuad                  = 0x2000 // En Quad
	runeEmQuad                  = 0x2001 // Em Quad
	runeEnSpace                 = 0x2002 // En Space
	runeEmSpace                 = 0x2003 // Em Space
	runeThreePerEmSpace         = 0x2004 // Three-Per-Em Space
	runeFourPerEmSpace          = 0x2005 // Four-Per-Em Space
	runeSixPerEmSpace           = 0x2006 // Six-Per-Em Space
	runeFigureSpace             = 0x2007 // Figure Space
	runePunctuationSpace        = 0x2008 // Punctuation Space
	runeThinSpace               = 0x2009 // Thin Space
	runeHairSpace               = 0x200A // Hair Space
	runeNarrowNoBreakSpace      = 0x202F // Narrow No-Break Space
	runeMediumMathematicalSpace = 0x205F // Medium Mathematical Space
	runeLineSeparator           = 0x2028 // Line Separator
	runeParagraphSeparator      = 0x2029 // Paragraph Separator

	// 其他控制字符
	runeSoftHyphen              = 0x00AD // Soft Hyphen
	runeCombiningGraphemeJoiner = 0x034F // Combining Grapheme Joiner
	runeArabicLetterMark        = 0x061C // Arabic Letter Mark
	runeMongolianVowelSeparator = 0x180E // Mongolian Vowel Separator
	runeDEL                     = 0x007F // DEL character
)

// Windows 无效文件名字符
const invalidChars = `<>:"/\|?*` + "\x00"
