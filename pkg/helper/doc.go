// Package helper 提供文件名清理和标准化功能
//
// 本包主要用于处理用户上传的文件名，使其符合 Windows 文件系统要求。
// 它可以安全地处理来自各种来源的文件名，包括不同操作系统和用户输入。
//
// 主要功能：
//
//   - 移除无效字符（<, >, :, ", /, \, |, ?, *, null 字符等）
//   - 移除控制字符和 Unicode 控制码
//   - 标准化空格字符（合并连续空格）
//   - 处理 Windows 保留的设备名（CON, PRN, AUX, NUL, COM1-9, LPT1-9）
//   - 截断过长的文件名（Windows 限制为 255 字符）
//
// 基本用法：
//
//	import "github.com/tragoedia0722/repository/pkg/helper"
//
//	cleaned := helper.CleanFilename("test<>:file.txt")
//	// 结果: "test___file.txt"
//
//	cleaned = helper.CleanFilename("CON.txt")
//	// 结果: "CON_file.txt"
//
//	cleaned = helper.CleanFilename("测试文件.txt")
//	// 结果: "测试文件.txt" (保留 Unicode)
//
// 性能：
//
// 本包经过优化，适合高频调用场景：
//   - 典型场景: ~150-200ns/op
//   - 内存分配: 1-2 次
//   - 适合每秒数千次调用
//
// 兼容性：
//
//   - 支持所有主流操作系统（Windows, Linux, macOS）
//   - 正确处理 Unicode 文件名
//   - 符合 Windows 文件系统要求
//
// Windows 文件名限制：
//
//   - 最大长度: 255 字符
//   - 不能包含: <, >, :, ", /, \, |, ?, *, 等
//   - 不能是保留设备名（不分大小写）: CON, PRN, AUX, NUL, COM1-9, LPT1-9
//   - 不能以空格或点结尾
//
// 测试：
//
// 运行测试：
//
//	go test ./pkg/helper/... -v
//
// 运行基准测试：
//
//	go test ./pkg/helper/... -bench=. -benchmem
package helper
