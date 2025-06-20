package helper

import (
	"strings"
	"unicode"
)

const invalidChars = `/` + "\x00"

func CleanFilename(filename string) string {
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
			return -1

		case !unicode.IsPrint(r):
			return -1
		}

		return r
	}, filename)

	cleaned = strings.Join(strings.Fields(cleaned), " ")
	cleaned = strings.TrimSpace(cleaned)

	cleaned = HandleReservedNames(cleaned)

	return cleaned
}

func HandleReservedNames(filename string) string {
	if filename == "" {
		return ""
	}

	lowerName := strings.ToLower(filename)
	nameWithoutExt := lowerName
	extName := ""

	if dotIndex := strings.LastIndex(lowerName, "."); dotIndex > 0 {
		nameWithoutExt = lowerName[:dotIndex]
		extName = filename[dotIndex:]
	}

	reservedNames := []string{"con", "prn", "aux", "nul"}
	for _, reserved := range reservedNames {
		if nameWithoutExt == reserved {
			return reserved + "_file" + extName
		}
	}

	if strings.HasPrefix(nameWithoutExt, "com") || strings.HasPrefix(nameWithoutExt, "lpt") {
		if len(nameWithoutExt) == 4 {
			suffix := nameWithoutExt[3:]
			if len(suffix) == 1 && suffix[0] >= '1' && suffix[0] <= '9' {
				return nameWithoutExt + "_file" + extName
			}
		}
	}

	return filename
}
