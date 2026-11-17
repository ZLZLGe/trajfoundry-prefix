# TrajFoundry Prefix

对已经标准化的轨迹再次做前缀去重，减少后续重复分类。输入仍是现有的标准化 JSON/JSONL，输出保留所选原文件的字节内容，不新增、删除或改写轨迹字段。

比较投影只用于证明前缀覆盖，不能作为输出。消息不同的真实分支保留；按当前分类用途，附加信息只检查工具定义，子 agent、压缩信息、媒体映射及来源扩展等不阻止合并。每条删除记录单独记录最终保留文件，核验记录不写进轨迹。

业务验证使用 Spark Cluster 和用户指定的队列，Driver/Executor 使用共享文件系统，输出保留在共享目录。验证数据及实际请求不进入 Git 仓库。参见 [比较规则](docs/rules.md) 和 [Cluster 提交说明](docs/cluster.md)。

```bash
uv sync --extra spark --extra test
uv run pytest
```

输入是 JSONL 清单，每行对应一个现有标准化文件，包含 `dataset`、`date`、`path`、`bytes`、`sha256`，以及 `uri`（S3 路径）或 `local_path`（共享绝对路径）。`bytes` 和 `sha256` 来自原标准化 manifest。当前各渠道均是一文件一个完整轨迹对象；多个轨迹共用一个物理分片时，需要先建立逐轨迹输入索引。

`dataset` 用于隔离渠道／数据集，归档集合的 `date` 可填 `archive`。本地文件缺失时可按同一清单的 `uri` 读取 S3，仍必须通过原始字节数和 SHA256 校验。

真实业务运行后，使用 [独立核验与残余检查](docs/validation.md) 检查每个保留文件和每条删除关系。

分组条件、Spark 分区方式和大 `_sub_N` 编号的真实例子见 [分组说明](docs/grouping.md)。

617,853 条真实标准化轨迹的渠道覆盖、合并数量、独立核验及残余边界见 [2026-11-17 验证报告](docs/validation-20261117.md)。

2026-10-10、10-11、10-12 的完整日期验证覆盖 12,491,591 条轨迹，不抽样，见 [完整日期验证报告](docs/validation-oct10-12-full.md)。

2026-10-10 全部 6,499,393 条轨迹取消顶层工具定义约束后，净多合并 23,643 条；两种策略均通过全量独立核验，见 [只按 messages 比较的完整对比](docs/validation-oct10-messages-only.md)。
