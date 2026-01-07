package helper

// charAction 表示对字符应该执行的操作
type charAction int

const (
	actionKeep                  charAction = iota // 保留字符
	actionRemove                                  // 移除字符
	actionReplaceWithSpace                        // 替换为空格
	actionReplaceWithUnderscore                   // 替换为下划线
)

// Result 包含文件名清理的结果信息
// 注意：当前未使用，保留供将来扩展
type Result struct {
	Original string // 原始文件名
	Cleaned  string // 清理后的文件名
	Modified bool   // 是否被修改
	Removed  int    // 移除的字符数
	Replaced int    // 替换的字符数
}

// String 返回清理结果的字符串表示
func (r *Result) String() string {
	return r.Cleaned
}
