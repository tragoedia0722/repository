# Repository

## 使用示例

```golang
package main  

import (  
    "context"  
    "fmt"  
    "git.hificloud.net/nas2024/cloud/demeter/repository/pkg/extractor"  
    "git.hificloud.net/nas2024/cloud/demeter/repository/pkg/importer"  
    "git.hificloud.net/nas2024/cloud/demeter/repository/pkg/repository"  
)  

func main() {  
    // 创建仓库  
    repo, err := repository.NewRepository("./my_repo")  
    if err != nil {  
        panic(err)  
    }  

    // 导入文件  
    imp := importer.NewImporter(repo.BlockStore(), "./source_files").WithProgress(func(completed, total int64, currentFile string) {  
        fmt.Printf("导入中: %s - %.2f%%\n", currentFile, float64(completed)/float64(total)*100)  
    })  

    result, err := imp.Import(context.Background())  
    if err != nil {  
        panic(err)  
    }  

    fmt.Printf("导入完成，根CID: %s\n", result.RootCid)  

    // 提取文件  
    ext := extractor.NewExtractor(repo.BlockStore(), result.RootCid, "./destination_files").WithProgress(func(completed, total int64, currentFile string) {  
        fmt.Printf("提取中: %s - %.2f%%\n", currentFile, float64(completed)/float64(total)*100)  
    })  

    err = ext.Extract(context.Background(), true)  
    if err != nil {  
        panic(err)  
    }  
    
    fmt.Println("提取完成")  
    
    // 关闭仓库  
    _ = repo.Close()  
}  
```

## API
### 文件仓库
```golang
// 创建新的仓库  
repo, err := repository.NewRepository("路径")  

// 获取块存储接口  
blockStore := repo.BlockStore()

// 检查是否具有该块
has, err := repo.HasBlock(context.TODO(), "mock_root_cid")

// 获取块的原数据
bytes, err := repo.GetRawData(context.TODO(), "mock_root_cid")

// 关闭仓库  
repo.Close()
```

### 文件导入
```golang
// 创建导入器  
imp := importer.NewImporter(blockStore, "源文件路径")  

// 添加进度回调  
imp.WithProgress(func(completed, total int64, currentFile string) {  
    // 处理进度更新  
})  

// 执行导入  
result, err := imp.Import(context.Background())  
```

### 文件提取
```golang
// 创建提取器  
ext := extractor.NewExtractor(blockStore, rootCid, "目标路径")  

// 添加进度回调  
ext.WithProgress(func(completed, total int64, currentFile string) {  
    // 处理进度更新  
})  

// 执行提取（参数为是否覆盖现有文件）  
err = ext.Extract(context.Background(), true)  
```