package helper

import (
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"unicode/utf8"
)

// TestEdgeCases 测试边界条件和极端情况
func TestEdgeCases(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		// 空值和单字符
		{
			name:     "empty string",
			input:    "",
			expected: "unnamed_file",
		},
		{
			name:     "single valid character",
			input:    "a",
			expected: "a",
		},
		{
			name:     "single invalid character becomes underscore",
			input:    "<",
			expected: "_",
		},
		{
			name:     "single space",
			input:    " ",
			expected: "unnamed_file",
		},
		{
			name:     "single dot",
			input:    ".",
			expected: "unnamed_file",
		},

		// 极端长度
		{
			name:     "very long filename",
			input:    strings.Repeat("a", 300) + ".txt",
			expected: strings.Repeat("a", 251) + ".txt",
		},
		{
			name:     "exactly 255 characters",
			input:    strings.Repeat("a", 251) + ".txt",
			expected: strings.Repeat("a", 251) + ".txt",
		},
		{
			name:     "256 characters - needs truncation",
			input:    strings.Repeat("a", 252) + ".txt",
			expected: strings.Repeat("a", 251) + ".txt",
		},

		// 特殊文件名模式
		{
			name:     "only dots",
			input:    "...",
			expected: "unnamed_file",
		},
		{
			name:     "only spaces",
			input:    "     ",
			expected: "unnamed_file",
		},
		{
			name:     "only underscores",
			input:    "___",
			expected: "___",
		},
		{
			name:     "only hyphens",
			input:    "---",
			expected: "---",
		},
		{
			name:     "hidden file starting with dot",
			input:    ".gitignore",
			expected: ".gitignore",
		},
		{
			name:     "multiple dots in filename",
			input:    "file.name.with.many.dots.txt",
			expected: "file.name.with.many.dots.txt",
		},

		// 全部为无效字符
		{
			name:     "all invalid characters",
			input:    `<>:"/\|?*`,
			expected: "_________",
		},
		{
			name:     "all control characters become underscore",
			input:    "\x00\x01\x02\x03\x04\x05",
			expected: "_",
		},

		// 混合场景
		{
			name:     "valid filename needs no changes",
			input:    "normal-file_123.txt",
			expected: "normal-file_123.txt",
		},
		{
			name:     "mixed unicode and ASCII",
			input:    "测试-test-файл-αβγ.txt",
			expected: "测试-test-файл-αβγ.txt",
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

// TestReservedNamesEdgeCases 测试保留名处理的边界情况
func TestReservedNamesEdgeCases(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		// 基本保留名
		{
			name:     "CON without extension",
			input:    "CON",
			expected: "CON_file",
		},
		{
			name:     "con lowercase",
			input:    "con",
			expected: "con_file",
		},
		{
			name:     "CON mixed case",
			input:    "CoN",
			expected: "CoN_file",
		},
		{
			name:     "PRN with extension",
			input:    "PRN.txt",
			expected: "PRN_file.txt",
		},

		// 边界情况：包含保留名但不是完全匹配
		{
			name:     "CON10 - not reserved",
			input:    "CON10",
			expected: "CON10",
		},
		{
			name:     "myCON - not reserved",
			input:    "myCON",
			expected: "myCON",
		},
		{
			name:     "CON.txt.file - not reserved",
			input:    "CON.txt.file",
			expected: "CON.txt.file",
		},
		{
			name:     "connect - not reserved",
			input:    "connect",
			expected: "connect",
		},
		{
			name:     "CON_file - already has suffix",
			input:    "CON_file",
			expected: "CON_file",
		},

		// 所有保留名
		{
			name:     "AUX",
			input:    "AUX",
			expected: "AUX_file",
		},
		{
			name:     "NUL",
			input:    "NUL",
			expected: "NUL_file",
		},
		{
			name:     "COM1",
			input:    "COM1",
			expected: "COM1_file",
		},
		{
			name:     "COM9",
			input:    "COM9",
			expected: "COM9_file",
		},
		{
			name:     "LPT1",
			input:    "LPT1",
			expected: "LPT1_file",
		},
		{
			name:     "LPT9",
			input:    "LPT9",
			expected: "LPT9_file",
		},

		// 保留名后跟各种扩展名
		{
			name:     "CON with multiple extensions",
			input:    "CON.tar.gz",
			expected: "CON.tar.gz", // .tar.gz 被认为是一个扩展名，CON.tar 不是保留名
		},
		{
			name:     "PRN with very long extension",
			input:    "PRN." + strings.Repeat("a", 100),
			expected: "PRN_file." + strings.Repeat("a", 100),
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

// TestUnicodeHandling 测试 Unicode 字符处理
func TestUnicodeHandling(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		// 各种 Unicode 脚本
		{
			name:     "Chinese characters",
			input:    "中文文件名.txt",
			expected: "中文文件名.txt",
		},
		{
			name:     "Japanese characters",
			input:    "日本語ファイル名.txt",
			expected: "日本語ファイル名.txt",
		},
		{
			name:     "Korean characters",
			input:    "한글파일이름.txt",
			expected: "한글파일이름.txt",
		},
		{
			name:     "Arabic characters",
			input:    "اسم الملف.txt",
			expected: "اسم الملف.txt",
		},
		{
			name:     "Hebrew characters",
			input:    "שם הקובץ.txt",
			expected: "שם הקובץ.txt",
		},
		{
			name:     "Cyrillic characters",
			input:    "имя_файла.txt",
			expected: "имя_файла.txt",
		},
		{
			name:     "Greek characters",
			input:    "όνομα_αρχείου.txt",
			expected: "όνομα_αρχείου.txt",
		},
		{
			name:     "Thai characters",
			input:    "ชื่อไฟล์.txt",
			expected: "ชื่อไฟล์.txt",
		},
		{
			name:     "Emoji in filename",
			input:    "file😀🎉.txt",
			expected: "file😀🎉.txt",
		},
		{
			name:     "Mixed scripts",
			input:    "test-测试-тест-😀.txt",
			expected: "test-测试-тест-😀.txt",
		},

		// Unicode 特殊字符（应该被移除）
		{
			name:     "LTR RTL marks removed",
			input:    "test\u200E\u200Ffile.txt",
			expected: "testfile.txt",
		},
		{
			name:     "zero width characters removed",
			input:    "test\u200B\u200C\u200Dfile.txt",
			expected: "testfile.txt",
		},
		{
			name:     "bidi embedding removed",
			input:    "test\u202A\u202B\u202C\u202D\u202Efile.txt",
			expected: "testfile.txt",
		},

		// Unicode 空格字符
		{
			name:     "non-breaking space to regular space",
			input:    "test\u00A0file.txt",
			expected: "test file.txt",
		},
		{
			name:     "ideographic space to regular space",
			input:    "test\u3000file.txt",
			expected: "test file.txt",
		},
		{
			name:     "thin space to regular space",
			input:    "test\u2009file.txt",
			expected: "test file.txt",
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

// TestSecurityScenarios 测试安全相关场景
func TestSecurityScenarios(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		// 路径遍历尝试（路径分隔符被替换为下划线）
		{
			name:     "path traversal with backslash",
			input:    "..\\..\\windows\\system32",
			expected: ".._.._windows_system32",
		},
		{
			name:     "path traversal with forward slash",
			input:    "../../../etc/passwd",
			expected: ".._.._.._etc_passwd",
		},
		{
			name:     "mixed path separators",
			input:    "folder\\file/name",
			expected: "folder_file_name",
		},

		// Windows 特殊路径
		{
			name:     "Windows drive letter",
			input:    "C:\\windows\\system32",
			expected: "C__windows_system32",
		},
		{
			name:     "UNC path",
			input:    "\\\\server\\share\\file.txt",
			expected: "__server_share_file.txt",
		},
		{
			name:     "relative path with dot",
			input:    "./config.json",
			expected: "._config.json",
		},
		{
			name:     "parent directory reference",
			input:    "../parent/file.txt",
			expected: ".._parent_file.txt",
		},

		// 命令注入尝试（通过特殊字符）
		{
			name:     "pipe character for command chaining",
			input:    "file.txt | malicious",
			expected: "file.txt _ malicious",
		},
		{
			name:     "redirect characters",
			input:    "file.txt > output.txt",
			expected: "file.txt _ output.txt",
		},
		// 注意：反引号不是 Windows 无效字符，会被保留
		{
			name:     "command substitution with backtick",
			input:    "file`rm -rf`.txt",
			expected: "file`rm -rf`.txt",
		},

		// SQL 注入尝试（单引号不是无效字符，会被保留）
		{
			name:     "SQL injection attempt",
			input:    "file'; DROP TABLE users; --.txt",
			expected: "file'; DROP TABLE users; --.txt",
		},

		// XSS 尝试（单引号会被保留）
		{
			name:     "XSS attempt with script tags",
			input:    "<script>alert('xss')</script>.txt",
			expected: "_script_alert('xss')__script_.txt",
		},

		// 保留设备名
		{
			name:     "reserved device name CON",
			input:    "CON",
			expected: "CON_file",
		},
		{
			name:     "reserved device name with path separator",
			input:    "..\\CON",
			expected: ".._CON", // 先处理路径分隔符，..CON 不是保留名
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

// TestConcurrentAccess 测试并发安全性
func TestConcurrentAccess(t *testing.T) {
	// 准备测试数据
	testCases := []string{
		"normal file.txt",
		"file<>:\"/\\|?*name.txt",
		"CON.txt",
		"测试文件.txt",
		strings.Repeat("a", 300) + ".txt",
		"file\u00A0\u3000name.txt",
	}

	// 并发测试
	numGoroutines := 100
	numIterations := 100

	var wg sync.WaitGroup
	errors := make(chan error, numGoroutines*numIterations)

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(goroutineID int) {
			defer wg.Done()
			for j := 0; j < numIterations; j++ {
				testCase := testCases[j%len(testCases)]
				result := CleanFilename(testCase)

				// 验证结果是有效的 UTF-8
				if !utf8.ValidString(result) {
					errors <- fmt.Errorf("goroutine %d, iteration %d: invalid UTF-8 result: %q",
						goroutineID, j, result)
					continue
				}

				// 验证结果长度不超过最大值
				if len(result) > MaxFilenameLength {
					errors <- fmt.Errorf("goroutine %d, iteration %d: result too long: %d",
						goroutineID, j, len(result))
					continue
				}

				// 验证结果不为空
				if result == "" {
					errors <- fmt.Errorf("goroutine %d, iteration %d: empty result",
						goroutineID, j)
					continue
				}
			}
		}(i)
	}

	wg.Wait()
	close(errors)

	// 检查是否有错误
	for err := range errors {
		t.Error(err)
	}
}

// TestBatchProcessing 测试批量处理性能和正确性
func TestBatchProcessing(t *testing.T) {
	// 生成大量测试文件名
	numFiles := 10000
	filenames := make([]string, numFiles)

	patterns := []string{
		"normal_file_%d.txt",
		"file<>:\"/\\|?*_%d.txt",
		"CON_%d.txt",
		"测试_%d.txt",
		"file\u00A0name_%d.txt",
		"%s.txt",
	}

	for i := 0; i < numFiles; i++ {
		pattern := patterns[i%len(patterns)]
		if pattern == "%s.txt" {
			filenames[i] = strings.Repeat("a", i%300) + ".txt"
		} else {
			filenames[i] = fmt.Sprintf(pattern, i)
		}
	}

	// 批量处理
	results := make([]string, numFiles)
	for i, filename := range filenames {
		results[i] = CleanFilename(filename)
	}

	// 验证结果
	for i, result := range results {
		// 验证有效的 UTF-8
		if !utf8.ValidString(result) {
			t.Errorf("Result %d is not valid UTF-8: %q", i, result)
		}

		// 验证长度限制
		if len(result) > MaxFilenameLength {
			t.Errorf("Result %d exceeds max length: %d", i, len(result))
		}

		// 验证不为空
		if result == "" {
			t.Errorf("Result %d is empty", i)
		}
	}
}

// TestTruncationEdgeCases 测试截断的边界情况
func TestTruncationEdgeCases(t *testing.T) {
	tests := []struct {
		name      string
		filename  string
		maxLength int
		check     func(result string, maxLength int) bool
	}{
		{
			name:      "truncate in middle of multibyte UTF-8",
			filename:  "文件名称.txt",
			maxLength: 8,
			check: func(result string, maxLength int) bool {
				return utf8.ValidString(result) && len(result) <= maxLength
			},
		},
		{
			name:      "truncate emoji",
			filename:  "file😀😁😂.txt",
			maxLength: 10,
			check: func(result string, maxLength int) bool {
				return utf8.ValidString(result) && len(result) <= maxLength
			},
		},
		{
			name:      "empty filename",
			filename:  "",
			maxLength: 100,
			check: func(result string, maxLength int) bool {
				return result == ""
			},
		},
		{
			name:      "filename longer than maxLength",
			filename:  strings.Repeat("a", 300) + ".txt",
			maxLength: 50,
			check: func(result string, maxLength int) bool {
				return len(result) <= maxLength && strings.HasSuffix(result, ".txt")
			},
		},
		{
			name:      "maxLength is 0",
			filename:  "test.txt",
			maxLength: 0,
			check: func(result string, maxLength int) bool {
				return result == ""
			},
		},
		{
			name:      "maxLength is 1",
			filename:  "test.txt",
			maxLength: 1,
			check: func(result string, maxLength int) bool {
				return len(result) <= 1
			},
		},
		{
			name:      "filename exactly maxLength",
			filename:  "test.txt",
			maxLength: 8,
			check: func(result string, maxLength int) bool {
				return result == "test.txt"
			},
		},
		{
			name:      "hidden file starting with dot",
			filename:  ".hiddenfile",
			maxLength: 5,
			check: func(result string, maxLength int) bool {
				return strings.HasPrefix(result, ".") && len(result) <= maxLength
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := TruncateFilename(tt.filename, tt.maxLength)
			if !tt.check(result, tt.maxLength) {
				t.Errorf("TruncateFilename(%q, %d) = %q failed check",
					tt.filename, tt.maxLength, result)
			}
		})
	}
}

// TestSpecialCharacters 测试特殊字符处理
func TestSpecialCharacters(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		// Windows 无效字符
		{input: "file<name>.txt", expected: "file_name_.txt"},
		{input: "file>name>.txt", expected: "file_name_.txt"},
		{input: "file:name:.txt", expected: "file_name_.txt"},
		{input: `file"name".txt`, expected: "file_name_.txt"},
		{input: "file/name/.txt", expected: "file_name_.txt"},
		{input: "file\\name\\.txt", expected: "file_name_.txt"},
		{input: "file|name|.txt", expected: "file_name_.txt"},
		{input: "file?name?.txt", expected: "file_name_.txt"},
		{input: "file*name*.txt", expected: "file_name_.txt"},

		// 控制字符
		{input: "file\x00name.txt", expected: "file_name.txt"},
		{input: "file\x01name.txt", expected: "filename.txt"},
		{input: "file\x1Fname.txt", expected: "filename.txt"},
		{input: "file\x7Fname.txt", expected: "filename.txt"},

		// 连续特殊字符
		{input: "file<>name.txt", expected: "file__name.txt"},
		{input: "file<<>>name.txt", expected: "file____name.txt"},
		{input: "file\"\"name.txt", expected: "file__name.txt"},

		// 混合特殊字符（9个特殊字符）
		{input: "file<>:\"/\\|?*name.txt", expected: "file_________name.txt"},

		// 特殊字符在开头和结尾（下划线会被保留）
		{input: "<filename>", expected: "_filename_"},
		{input: "<filename>.txt", expected: "_filename_.txt"},
		{input: "filename<>", expected: "filename__"},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			result := CleanFilename(tt.input)
			if result != tt.expected {
				t.Errorf("CleanFilename(%q) = %q, want %q", tt.input, result, tt.expected)
			}
		})
	}
}

// TestSpacesAndDots 测试空格和点的处理
func TestSpacesAndDots(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		// 前导空格和点
		{input: " filename.txt", expected: "filename.txt"},
		{input: "  filename.txt", expected: "filename.txt"},
		{input: ".filename.txt", expected: ".filename.txt"},
		{input: "..filename.txt", expected: "..filename.txt"},

		// 尾随空格和点（.txt 是扩展名，会被保留）
		{input: "filename.txt ", expected: "filename.txt"},
		{input: "filename.txt  ", expected: "filename.txt"},
		{input: "filename.txt.", expected: "filename.txt"},
		{input: "filename.txt..", expected: "filename.txt"},

		// 多个空格
		{input: "file   name.txt", expected: "file name.txt"},
		{input: "file     name.txt", expected: "file name.txt"},
		{input: "file name  test.txt", expected: "file name test.txt"},

		// Unicode 空格
		{input: "file\u00A0name.txt", expected: "file name.txt"},
		{input: "file\u3000name.txt", expected: "file name.txt"},
		{input: "file\u2009name.txt", expected: "file name.txt"},

		// 混合空格
		{input: "file \u00A0 \u3000 name.txt", expected: "file name.txt"},

		// 点和空格混合
		{input: "file. name.txt", expected: "file. name.txt"},
		{input: "file .name.txt", expected: "file .name.txt"},
		{input: "file. .name.txt", expected: "file. .name.txt"},
		{input: "file...  .  .txt", expected: "file... . .txt"},
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

// TestReservedNamesFunction 测试 ReservedNames 辅助函数
func TestReservedNamesFunction(t *testing.T) {
	names := ReservedNames()

	// 验证返回了所有预期的保留名
	expectedNames := []string{
		"CON", "PRN", "AUX", "NUL",
		"COM1", "COM2", "COM3", "COM4", "COM5",
		"COM6", "COM7", "COM8", "COM9",
		"LPT1", "LPT2", "LPT3", "LPT4", "LPT5",
		"LPT6", "LPT7", "LPT8", "LPT9",
	}

	if len(names) != len(expectedNames) {
		t.Errorf("ReservedNames() returned %d names, expected %d", len(names), len(expectedNames))
	}

	// 创建映射以便检查
	nameMap := make(map[string]bool)
	for _, name := range names {
		nameMap[name] = true
	}

	for _, expected := range expectedNames {
		if !nameMap[expected] {
			t.Errorf("ReservedNames() missing %q", expected)
		}
	}
}

// TestRealWorldScenarios 测试真实世界场景
func TestRealWorldScenarios(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		// 用户上传的文件
		{input: "Screenshot 2024-01-15 at 3.45.56 PM.png", expected: "Screenshot 2024-01-15 at 3.45.56 PM.png"},
		{input: "My Photo (1).jpg", expected: "My Photo (1).jpg"},
		{input: "Document - Final Version.docx", expected: "Document - Final Version.docx"},

		// 下载的文件
		{input: "file%20name%20with%20spaces.pdf", expected: "file%20name%20with%20spaces.pdf"},
		{input: "downloaded_file[1].txt", expected: "downloaded_file[1].txt"},

		// 生成的文件名
		{input: "import_2024-01-15_123456.csv", expected: "import_2024-01-15_123456.csv"},
		{input: "export-data-2024Q1.xlsx", expected: "export-data-2024Q1.xlsx"},

		// 数字化文件
		{input: "scan_001_page_1.jpg", expected: "scan_001_page_1.jpg"},
		{input: "IMG_20240115_143522.jpg", expected: "IMG_20240115_143522.jpg"},

		// 带有特殊字符的文件名
		{input: "Report: Q1 2024.pdf", expected: "Report_ Q1 2024.pdf"},
		{input: "Presentation/Meeting/Notes.docx", expected: "Presentation_Meeting_Notes.docx"},
		{input: `File "Quoted" Name.txt`, expected: "File _Quoted_ Name.txt"},

		// 国际化文件名
		{input: "café document.txt", expected: "café document.txt"},
		{input: "naïve file.txt", expected: "naïve file.txt"},
		{input: "日本語のファイル.txt", expected: "日本語のファイル.txt"},
		{input: "Файл на русском.txt", expected: "Файл на русском.txt"},
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

// TestHandleReservedNamesDirect 直接测试 HandleReservedNames 函数
func TestHandleReservedNamesDirect(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{input: "", expected: ""},
		{input: "CON", expected: "CON_file"},
		{input: "con", expected: "con_file"},
		{input: "CON.txt", expected: "CON_file.txt"},
		{input: "normal.txt", expected: "normal.txt"},
		{input: "CON10", expected: "CON10"},
		{input: ".gitignore", expected: ".gitignore"},
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

// TestFilenameProperties 测试清理后的文件名属性
func TestFilenameProperties(t *testing.T) {
	testInputs := []string{
		"normal file.txt",
		"file<>:\"/\\|?*name.txt",
		"CON.txt",
		"测试文件.txt",
		strings.Repeat("a", 300) + ".txt",
		"file\u00A0\u3000name.txt",
		"file\x00\x01\x02name.txt",
		"   filename.txt   ",
		"filename.txt...",
	}

	for _, input := range testInputs {
		t.Run(input, func(t *testing.T) {
			result := CleanFilename(input)

			// 验证有效的 UTF-8
			if !utf8.ValidString(result) {
				t.Errorf("Result is not valid UTF-8: %q", result)
			}

			// 验证长度限制
			if len(result) > MaxFilenameLength {
				t.Errorf("Result exceeds max length %d: %d", MaxFilenameLength, len(result))
			}

			// 验证不为空（除非输入全是空格等）
			if result == "" {
				t.Errorf("Result is empty for input: %q", input)
			}

			// 验证不包含 Windows 无效字符
			invalidChars := `<>:"/\|?*` + "\x00"
			for _, c := range invalidChars {
				if strings.ContainsRune(result, c) {
					t.Errorf("Result contains invalid character %q: %q", c, result)
				}
			}

			// 验证不以空格或点结尾
			if len(result) > 0 {
				lastChar := result[len(result)-1]
				if lastChar == ' ' || lastChar == '.' {
					t.Errorf("Result ends with space or dot: %q", result)
				}
			}
		})
	}
}

// TestPathSeparatorHandling 测试路径分隔符处理
func TestPathSeparatorHandling(t *testing.T) {
	tests := []struct {
		name  string
		input string
	}{
		{"Windows path", `C:\Users\test\file.txt`},
		{"Unix path", `/home/user/file.txt`},
		{"Mixed separators", `folder\subfolder/file.txt`},
		{" UNC path", `\\server\share\file.txt`},
		{"Relative path", `..\..\file.txt`},
		{"Current directory", `.\file.txt`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := CleanFilename(tt.input)

			// 路径分隔符应该被替换为下划线
			if strings.Contains(result, "/") || strings.Contains(result, "\\") {
				t.Errorf("Result contains path separator: %q", result)
			}

			// 验证是有效的文件名（不包含路径）
			base := filepath.Base(result)
			if base != result {
				t.Errorf("Result looks like a path: %q (base: %q)", result, base)
			}
		})
	}
}
