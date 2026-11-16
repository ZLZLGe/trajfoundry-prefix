# TrajFoundry Prefix

对已经标准化的轨迹再次做前缀去重，减少后续重复分类。输入仍是现有的标准化 JSON/JSONL，输出保留所选原文件的字节内容，不新增、删除或改写轨迹字段。

比较投影只用于证明前缀覆盖，不能作为输出。真实分支全部保留；短轨迹里的工具定义、子 agent、媒体和其他独有信息不能因去重而消失。每条删除记录单独记录最终保留文件，核验记录不写进轨迹。

支持本地 Spark；分布式 Spark 使用 Driver/Executor 都可见的共享文件系统。验证数据及实际请求不进入 Git 仓库。完整规则、运行命令和实测报告将随验证提交。

```bash
uv sync --extra spark --extra test
uv run pytest
```
