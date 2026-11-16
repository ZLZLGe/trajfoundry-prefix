# TrajFoundry Prefix

对已经标准化的轨迹再次做前缀去重，减少后续重复分类。输入仍是现有的标准化 JSON/JSONL，输出保留所选原文件的字节内容，不新增、删除或改写轨迹字段。

比较投影只用于证明前缀覆盖，不能作为输出。真实分支全部保留；短轨迹里的工具定义、子 agent、媒体和其他独有信息不能因去重而消失。每条删除记录单独记录最终保留文件，核验记录不写进轨迹。

业务验证使用 Spark Cluster 和用户指定的队列，Driver/Executor 使用共享文件系统，输出保留在共享目录。验证数据及实际请求不进入 Git 仓库。参见 [比较规则](docs/rules.md) 和 [Cluster 提交说明](docs/cluster.md)。

```bash
uv sync --extra spark --extra test
uv run pytest
```
