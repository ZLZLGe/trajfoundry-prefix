# SXF v004 与模型筛选 v002 发布结果

已于 2026-11-17 完成发布，范围为 SXF `dt=2026-08-21` 的完整分区。

- 标准化轨迹：**108,531 → 90,589**，去掉重复前缀 17,942 条（16.53%），保留文件共 95,010,075,414 字节（约 95.01 GB）。
- 三项指标筛选：**45,678 → 33,191**，减少 12,487 条（27.34%），共 68,900,566,222 字节（约 68.90 GB）。
- 三项阈值均包含等于：`total_tokens >= 50000`、`total_tool_calls >= 20`、`total_rounds >= 20`。最后一项是根轨迹 messages 中的 assistant 数。

## 输出位置

标准化目录：

`s3://agent-trajectory/lakehouse/SXF/mul-agent-sxf/normalized/v004/dt=2026-08-21/`

筛选结果沿用原来的指标前缀，在各模型下使用 `v002/`：

- `s3://agent-trajectory/lakehouse/SXF/mul-agent-sxf/tokens_ge_50000_toolcalls_ge_20_assistants_ge_20/gpt-5.5/v002/`（8,755 条）
- `s3://agent-trajectory/lakehouse/SXF/mul-agent-sxf/tokens_ge_50000_toolcalls_ge_20_assistants_ge_20/gpt-5.5-codex/v002/`（15 条）
- `s3://agent-trajectory/lakehouse/SXF/mul-agent-sxf/tokens_ge_50000_toolcalls_ge_20_assistants_ge_20/gpt-5.5-high/v002/`（1 条）
- `s3://agent-trajectory/lakehouse/SXF/mul-agent-sxf/tokens_ge_50000_toolcalls_ge_20_assistants_ge_20/gpt-5.5-openai-compact/v002/`（11 条）
- `s3://agent-trajectory/lakehouse/SXF/mul-agent-sxf/tokens_ge_50000_toolcalls_ge_20_assistants_ge_20/gpt-5.5-turbo/v002/`（2 条）
- `s3://agent-trajectory/lakehouse/SXF/mul-agent-sxf/tokens_ge_50000_toolcalls_ge_20_assistants_ge_20/gpt-5.6-sol/v002/`（24,407 条）

目录示例：

```text
s3://agent-trajectory/lakehouse/SXF/mul-agent-sxf/
├── normalized/
│   ├── v003/                         # 旧版保留
│   └── v004/
│       └── dt=2026-08-21/
│           ├── manifest.json
│           ├── lineage.jsonl
│           └── <原轨迹文件名>.jsonl  # 共 90,589 条
└── tokens_ge_50000_toolcalls_ge_20_assistants_ge_20/
    ├── gpt-5.5/
    ├── gpt-5.5-codex/
    │   ├── v001/                     # 旧版保留
    │   └── v002/
    │       ├── manifest.json
    │       └── dt=2026-08-21/
    │           └── <原轨迹文件名>.jsonl
    ├── gpt-5.5-high/
    ├── gpt-5.5-openai-compact/
    ├── gpt-5.5-turbo/
    └── gpt-5.6-sol/                   # 其余模型目录结构相同
```

## 各模型数量

| 模型 | 标准化 v003 | 标准化 v004 | 筛选 v001 | 筛选 v002 |
| --- | ---: | ---: | ---: | ---: |
| gpt-5.5 | 36,354 | 30,349 | 12,887 | 8,755 |
| gpt-5.5-codex | 133 | 130 | 15 | 15 |
| gpt-5.5-high | 30 | 30 | 1 | 1 |
| gpt-5.5-latest | 34 | 34 | 0 | 0 |
| gpt-5.5-openai-compact | 378 | 371 | 11 | 11 |
| gpt-5.5-turbo | 60 | 60 | 2 | 2 |
| gpt-5.6-sol | 71,539 | 59,612 | 32,762 | 24,407 |
| gpt-5.6-sol-xhigh | 3 | 3 | 0 | 0 |
| 合计 | 108,531 | 90,589 | 45,678 | 33,191 |

`gpt-5.5-latest` 和 `gpt-5.6-sol-xhigh` 仍在标准化目录中；它们没有同时达到三项阈值的轨迹，因此没有筛选 v002 目录。

## 处理口径与文件

复用已经全量审计通过的 `messages_only` 去留关系：保持身份分组和现有鲁棒比较规则，比较时不考虑 reasoning 与顶层 tools 定义。每条被合并轨迹都已有最终保留轨迹作为覆盖证据；90,589 条保留集第二遍比较新增合并为 0。本次发布没有继续放宽合并条件。

保留轨迹的原文件名和全部字节内容不变，reasoning、子轨迹、工具定义和已有质量字段都保留。子轨迹不单独计数；筛选直接使用根轨迹的三个指标，不按 accepted、tool_defs_tag 等质量标记排除数据。

标准化 manifest 保持原 `trajfoundry-v4` 契约，输出数量、字节和文件哈希重新生成；lineage 合并到最终保留轨迹并保留全部原始来源。源目录中的旧诊断文件保留在 v003。

筛选 manifest 仍只有 9 个字段，不增加分片索引。`model` 是模型；`source` 是新版标准化根目录；三个 `min_*` 是包含等于的筛选阈值；`date_start`、`date_end` 是覆盖的业务日期；`trajectory_count` 和 `total_bytes` 是该模型筛选轨迹的条数与原文件总字节数。

`gpt-5.5-codex/v002/manifest.json` 的实际内容：

```json
{
  "date_end": "2026-08-21",
  "date_start": "2026-08-21",
  "min_assistant_steps": 20,
  "min_tokens": 50000,
  "min_tool_calls": 20,
  "model": "gpt-5.5-codex",
  "source": "s3://agent-trajectory/lakehouse/SXF/mul-agent-sxf/normalized/v004/",
  "total_bytes": 19323092,
  "trajectory_count": 15
}
```

## 核验与代码

- 全部 108,531 条源文件通过 SHA256 核验；90,589 条保留轨迹通过文件名、标准格式和派生字段校验。
- 全部 90,589 条标准化目标、33,191 条筛选目标均完整读取并核对 SHA256，目标文件集合和大小与清单一致。
- 新 lineage 共 90,589 行，保留 648,069 个唯一原始来源；加上原有 19,929 条隔离原始记录和 67 条解析失败，仍对应 668,065 条原始输入。
- 完成后回读 7 个 S3 manifest 均与发布回执一致，源 v003 manifest 哈希保持不变。
- 实际 Spark Driver 已 `Succeeded`，退出码 0；队列为 `root.datacenter.pjcc.agent_trajectory`，16 个 Executor × 8 核、512 分区。本次成功发布 Driver 运行约 7 分 6 秒。

GitHub 仓库：`https://github.com/ZLZLGe/trajfoundry-prefix`。

- `46a323b`：增加独立的 S3 发布入口，支持全量验证、来源合并、原文件复制、按模型筛选以及失败续跑。
- `69621ca`：把源目录已有诊断文件纳入冻结的目录清单校验；仍拒绝未知文件或大小变化。6 项发布测试通过。

实际发布源码固定为 `69621ca`，未改生产调度配置。

内部核验回执目录：`/share/gezhilong/trajfoundry-prefix-runs/20261117-sxf-v004-publication/receipts-02/`。其中 `report.json` 是汇总，`post-publication-check.json` 是 S3 回读核验；Driver 回执为 `/share/gezhilong/trajfoundry-prefix-runs/20261117-sxf-v004-publication/control-02/final.json`。前序第一次提交在源目录清单检查时发现旧诊断文件后停止，没有写入 S3；修正后用独立控制目录完成了本次发布。
