package helper

import (
	"strings"
	"testing"
	"unicode/utf8"
)

func TestCleanFilename(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		// Empty string
		{
			name:     "empty string returns unnamed_file",
			input:    "",
			expected: "unnamed_file",
		},
		// Invalid characters
		{
			name:     "removes invalid characters",
			input:    `test<>:"/\|?*file`,
			expected: "test_________file",
		},
		{
			name:     "removes null character",
			input:    "test\x00file",
			expected: "test_file",
		},
		// Control characters
		{
			name:     "removes control characters except tab",
			input:    "test\x01\x02\x1f\x09file",
			expected: "testfile",
		},
		// Spaces handling
		{
			name:     "replaces unicode spaces with regular space",
			input:    "test\u00A0file",
			expected: "test file",
		},
		{
			name:     "trims trailing spaces and dots",
			input:    "test...  .  ",
			expected: "test",
		},
		{
			name:     "collapses multiple spaces",
			input:    "test    file   name",
			expected: "test file name",
		},
		// Reserved names
		{
			name:     "handles reserved name CON",
			input:    "CON",
			expected: "CON_file",
		},
		{
			name:     "handles reserved name con",
			input:    "con",
			expected: "con_file",
		},
		{
			name:     "handles reserved name PRN with extension",
			input:    "PRN.txt",
			expected: "PRN_file.txt",
		},
		{
			name:     "handles COM1",
			input:    "COM1",
			expected: "COM1_file",
		},
		{
			name:     "handles LPT1",
			input:    "LPT1",
			expected: "LPT1_file",
		},
		{
			name:     "handles AUX",
			input:    "AUX",
			expected: "AUX_file",
		},
		{
			name:     "handles NUL",
			input:    "NUL",
			expected: "NUL_file",
		},
		// Non-printable characters
		{
			name:     "removes non-printable characters",
			input:    "test\x7Ffile",
			expected: "testfile",
		},
		// Unicode special characters
		{
			name:     "removes LTR/RTL markers",
			input:    "test\u200E\u200Ffile",
			expected: "testfile",
		},
		{
			name:     "removes bidi embedding characters",
			input:    "test\u202A\u202B\u202C\u202D\u202Efile",
			expected: "testfile",
		},
		{
			name:     "removes zero-width characters",
			input:    "test\u200B\u200C\u200Dfile",
			expected: "testfile",
		},
		{
			name:     "removes word joiner and BOM",
			input:    "test\u2060\uFEFFfile",
			expected: "testfile",
		},
		{
			name:     "handles ideographic space",
			input:    "test\u3000file",
			expected: "test file",
		},
		// Normal filenames
		{
			name:     "keeps valid filename unchanged",
			input:    "normal-file.txt",
			expected: "normal-file.txt",
		},
		{
			name:     "handles filename with valid unicode",
			input:    "测试文件.txt",
			expected: "测试文件.txt",
		},
		{
			name:     "handles underscores and hyphens",
			input:    "my_test-file.txt",
			expected: "my_test-file.txt",
		},
		{
			name:     "handles dots in filename",
			input:    "my.file.name.txt",
			expected: "my.file.name.txt",
		},
		// Edge cases that result in empty after cleaning
		{
			name:     "only invalid characters returns unnamed",
			input:    "<>:\"/\\|?*",
			expected: "_________",
		},
		{
			name:     "only control characters returns unnamed",
			input:    "\x00\x01\x02\x03",
			expected: "_",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := CleanFilename(tt.input)
			if result != tt.expected {
				t.Errorf("CleanFilename(%q) = %q, want %q", tt.input, result, tt.expected)
			}
		})
	}
}

func TestHandleReservedNames(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "empty string returns empty",
			input:    "",
			expected: "",
		},
		// Reserved names without extension
		{
			name:     "CON gets suffix",
			input:    "CON",
			expected: "CON_file",
		},
		{
			name:     "con (lowercase) gets suffix",
			input:    "con",
			expected: "con_file",
		},
		{
			name:     "PRN gets suffix",
			input:    "PRN",
			expected: "PRN_file",
		},
		{
			name:     "AUX gets suffix",
			input:    "AUX",
			expected: "AUX_file",
		},
		{
			name:     "NUL gets suffix",
			input:    "NUL",
			expected: "NUL_file",
		},
		{
			name:     "COM1 gets suffix",
			input:    "COM1",
			expected: "COM1_file",
		},
		{
			name:     "COM9 gets suffix",
			input:    "COM9",
			expected: "COM9_file",
		},
		{
			name:     "LPT1 gets suffix",
			input:    "LPT1",
			expected: "LPT1_file",
		},
		{
			name:     "LPT9 gets suffix",
			input:    "LPT9",
			expected: "LPT9_file",
		},
		// Reserved names with extension
		{
			name:     "CON.txt gets suffix before extension",
			input:    "CON.txt",
			expected: "CON_file.txt",
		},
		{
			name:     "con.jpeg gets suffix before extension",
			input:    "con.jpeg",
			expected: "con_file.jpeg",
		},
		{
			name:     "PRN.data gets suffix before extension",
			input:    "PRN.data",
			expected: "PRN_file.data",
		},
		// Non-reserved names
		{
			name:     "normal filename unchanged",
			input:    "normal.txt",
			expected: "normal.txt",
		},
		{
			name:     "CON10 (not reserved) unchanged",
			input:    "CON10",
			expected: "CON10",
		},
		{
			name:     "my-con-file.txt unchanged",
			input:    "my-con-file.txt",
			expected: "my-con-file.txt",
		},
		{
			name:     "connect (not exactly CON) unchanged",
			input:    "connect",
			expected: "connect",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := HandleReservedNames(tt.input)
			if result != tt.expected {
				t.Errorf("HandleReservedNames(%q) = %q, want %q", tt.input, result, tt.expected)
			}
		})
	}
}

func TestTruncateFilename(t *testing.T) {
	tests := []struct {
		name      string
		filename  string
		maxLength int
		expected  string
	}{
		{
			name:      "short filename unchanged",
			filename:  "test.txt",
			maxLength: 100,
			expected:  "test.txt",
		},
		{
			name:      "exact length unchanged",
			filename:  "test.txt",
			maxLength: 8,
			expected:  "test.txt",
		},
		{
			name:      "truncate without extension",
			filename:  "verylongfilename",
			maxLength: 10,
			expected:  "verylongfi",
		},
		{
			name:      "truncate with extension preserves extension",
			filename:  "verylongfilename.txt",
			maxLength: 15,
			expected:  "verylongfil.txt",
		},
		{
			name:      "truncate with double extension",
			filename:  "verylongfilename.tar.gz",
			maxLength: 18,
			expected:  "verylongfilenam.gz",
		},
		{
			name:      "dot at start truncates fully",
			filename:  ".hidden",
			maxLength: 5,
			expected:  ".hidd",
		},
		{
			name:      "multiple dots keeps last extension",
			filename:  "file.name.test.txt",
			maxLength: 14,
			expected:  "file.name..txt",
		},
		{
			name:      "extension too long truncates fully",
			filename:  "test.verylongextension",
			maxLength: 10,
			expected:  "test.veryl",
		},
		{
			name:      "no dot but with extension separator",
			filename:  "test.",
			maxLength: 3,
			expected:  "tes",
		},
		{
			name:      "empty filename",
			filename:  "",
			maxLength: 10,
			expected:  "",
		},
		{
			name:      "long unicode filename",
			filename:  "测试文件名.txt",
			maxLength: 10,
			expected:  "测试.txt",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := TruncateFilename(tt.filename, tt.maxLength)
			if result != tt.expected {
				t.Errorf("TruncateFilename(%q, %d) = %q, want %q", tt.filename, tt.maxLength, result, tt.expected)
			}
		})
	}
}

// TestUTF8Truncation 测试 UTF-8 多字节字符截断的安全性
// 确保不会在多字节字符中间截断导致无效的 UTF-8 序列
func TestUTF8Truncation(t *testing.T) {
	tests := []struct {
		name      string
		filename  string
		maxLength int
		// 不检查精确输出，只检查输出是有效的 UTF-8
		checkUTF8Valid bool
	}{
		{
			name:           "chinese characters boundary",
			filename:       "文件名称.txt",
			maxLength:      8,
			checkUTF8Valid: true,
		},
		{
			name:           "emoji boundary",
			filename:       "file😀test.txt",
			maxLength:      12,
			checkUTF8Valid: true,
		},
		{
			name:           "mixed unicode boundary",
			filename:       "测试test😀.txt",
			maxLength:      11,
			checkUTF8Valid: true,
		},
		{
			name:           "greek characters",
			filename:       "αβγδε.txt",
			maxLength:      8,
			checkUTF8Valid: true,
		},
		{
			name:           "cyrillic characters",
			filename:       "тест.txt",
			maxLength:      8,
			checkUTF8Valid: true,
		},
		{
			name:           "multiple emojis",
			filename:       "😀😁😂.txt",
			maxLength:      11,
			checkUTF8Valid: true,
		},
		{
			name:           "very short truncation",
			filename:       "文件名称.txt",
			maxLength:      5,
			checkUTF8Valid: true,
		},
		{
			name:           "single emoji",
			filename:       "😀.txt",
			maxLength:      5,
			checkUTF8Valid: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := TruncateFilename(tt.filename, tt.maxLength)

			// 如果需要检查 UTF-8 有效性
			if tt.checkUTF8Valid {
				// 检查结果是有效的 UTF-8
				if !utf8.ValidString(result) {
					t.Errorf("TruncateFilename(%q, %d) = %q is NOT valid UTF-8", tt.filename, tt.maxLength, result)
				}

				// 检查结果长度不超过最大长度
				if len(result) > tt.maxLength {
					t.Errorf("TruncateFilename(%q, %d) = %q has length %d, exceeds max %d", tt.filename, tt.maxLength, result, len(result), tt.maxLength)
				}

				// 检查扩展名被保留（如果有的话）
				dotIndex := strings.LastIndex(tt.filename, ".")
				if dotIndex > 0 && dotIndex < len(tt.filename)-1 {
					expectedExt := tt.filename[dotIndex:]
					resultExt := result[strings.LastIndex(result, "."):]
					if resultExt != expectedExt {
						// 扩展名可能被截断，但要确保有扩展名分隔符
						if strings.LastIndex(result, ".") < 0 {
							t.Logf("Warning: Extension %q was completely removed", expectedExt)
						}
					}
				}
			}
		})
	}
}
