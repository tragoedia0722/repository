package helper

import (
	"strings"
	"unicode"
)

const invalidChars = `<>:"/\|?*` + "\x00"

var reservedNames = []string{
	"con", "prn", "aux", "nul",
	"com1", "com2", "com3", "com4", "com5", "com6", "com7", "com8", "com9",
	"lpt1", "lpt2", "lpt3", "lpt4", "lpt5", "lpt6", "lpt7", "lpt8", "lpt9",
}

func CleanFilename(filename string) string {
	if filename == "" {
		return "unnamed_file"
	}

	cleaned := strings.Map(func(r rune) rune {
		switch {
		case r == 0x200E || r == 0x200F:
			return -1
		case r >= 0x202A && r <= 0x202E:
			return -1

		case r == 0x200B || r == 0x200C || r == 0x200D:
			return -1
		case r == 0x2060 || r == 0xFEFF:
			return -1

		case r == 0x00A0:
			return ' '
		case r == 0x3000:
			return ' '
		case r >= 0x2000 && r <= 0x200A:
			return ' '
		case r == 0x2007 || r == 0x2008 || r == 0x205F:
			return ' '

		case r == 0x00AD:
			return -1
		case r == 0x034F:
			return -1
		case r == 0x061C:
			return -1
		case r == 0x180E:
			return -1

		case r == 0x2028 || r == 0x2029:
			return ' '

		case strings.ContainsRune(invalidChars, r):
			return '_'

		case r >= 0 && r <= 31 && r != '\t':
			return -1

		case !unicode.IsPrint(r):
			return -1
		}

		return r
	}, filename)

	cleaned = strings.Join(strings.Fields(cleaned), " ")
	cleaned = strings.TrimSpace(cleaned)

	cleaned = strings.TrimRight(cleaned, ". ")

	cleaned = HandleReservedNames(cleaned)

	cleaned = TruncateFilename(cleaned, 255)

	if cleaned == "" {
		cleaned = "unnamed_file"
	}

	return cleaned
}

func HandleReservedNames(filename string) string {
	if filename == "" {
		return ""
	}

	lowerName := strings.ToLower(filename)
	nameWithoutExt := lowerName
	extName := ""

	if dotIndex := strings.LastIndex(filename, "."); dotIndex > 0 && dotIndex < len(filename)-1 {
		nameWithoutExt = strings.ToLower(filename[:dotIndex])
		extName = filename[dotIndex:]
	}

	for _, reserved := range reservedNames {
		if nameWithoutExt == reserved {
			originalName := filename
			if extName != "" {
				originalName = filename[:len(filename)-len(extName)]
			}
			return originalName + "_file" + extName
		}
	}

	return filename
}

func TruncateFilename(filename string, maxLength int) string {
	if len(filename) <= maxLength {
		return filename
	}

	dotIndex := strings.LastIndex(filename, ".")
	if dotIndex <= 0 || dotIndex == len(filename)-1 {
		return filename[:maxLength]
	}

	ext := filename[dotIndex:]
	name := filename[:dotIndex]

	minNameLength := 1
	maxNameLength := maxLength - len(ext)

	if maxNameLength < minNameLength {
		return filename[:maxLength]
	}

	return name[:maxNameLength] + ext
}
