# TokenPlan classification/50000-filter 全量消息前缀去重

使用 `messages_only` 对 `s3://agent-trajectory/lakehouse/token-plan/classification/50000-filter/` 的全部现存轨迹去重：**165 条 → 56 条，减少 109 条（66.06%）**。

后续对全部 56 条做了完整两两复查，发现脱敏和后台通知差异阻断的 11 条较短候选；实际保留数仍为 56。[复查结果及边界](review-tokenplan-56-retained.md)。

| 模型目录 | 完整输入 | 保留 | 合并 | 减少比例 |
|---|---:|---:|---:|---:|
| Atria-Dawn | 43 | 32 | 11 | 25.58% |
| Atria-Dawn-Preview | 122 | 24 | 98 | 80.33% |
| 合计 | 165 | 56 | 109 | 66.06% |

## 输入范围

根 manifest、statistics 和 S3 全目录 listing 一致：该前缀是命中具体科研方向的已分类子集，共 165 个一文件一轨迹的 JSONL 文件，另有 4 个说明／统计／流程元数据对象。完整输入为 103,986,094 字节，覆盖 2026-09-14 至 2026-10-30 的 17 个日期。直接读取此目录中的分类后原文件，所有分类、科研方向、评分等字段均随选中的原文件保留。

没有按用户请求哈希、分类标签或日期再次筛选。来源 statistics 声明的 65 个不同用户请求不是前缀去重的目标数量；相同请求可能有不同消息分支。

## 比较规则

使用五条已验证消息鲁棒规则及 `extra_information=messages_only`。顶层 tools、classification、research_area 等附加信息不阻止消息覆盖；messages 内的角色、实际正文、调用名、参数、结果及引用绑定仍按既有规则比较。沿用原规则对回放 reasoning 的处理。

按 TokenPlan 渠道、轨迹实际 model、harness、用户和 session 分组。两个模型不相互合并；同一真实 session 可在该前缀的不同日期间比较，无 session 按原日期隔离。`_sub_N` 不拆组。聚合保留完整原轨迹文件，不拼接消息或重写字段。

本次 109 条均为较短消息前缀被较长轨迹覆盖。独立核验确认，全部 109 条最终对应关系都需要工具调用 ID 一致更名规则；其中 4 条还存在较长文件未覆盖的顶层工具定义。后者是对应关系计数，不等于单独取消 tools 约束的净增合并量。

## 核验

- S3 全量 listing 与 manifest 的 165 项逐一对应；输入清单冻结 SHA256：`f18f74eba97871647b64ae83a0915c72e0bab01846bb528bc014af817be6f5cc`。
- 完整业务比较、独立核验和全部保留轨迹的第二遍比较，均通过 Spark Cluster 提交，队列 `root.datacenter.pjcc.agent_trajectory`，4 个 Executor × 2 核，分区参数 32。
- 全部 56 个实际输出文件的原始字节数和 SHA256 通过核验；全部 109 条合并关系由独立原始 JSON 比较器验证，并指向最终保留文件。
- 全部保留轨迹再次比较，新增合并 0 条。完成后重新 listing，源 manifest 与全部对象的 ETag、字节数保持一致。
- 执行代码为 `3c2ed18`；沿用已通过 100 项测试的实现。

## 产物

输出 36,134,903 字节，均位于共享文件系统；S3 只读。

```text
/share/gezhilong/trajfoundry-prefix-runs/20261117-tokenplan-50000-messages-only/
├── input-plan.jsonl                  # 全部 165 条原始 S3 文件引用
├── plan-complete.json                # 源 manifest、全量 listing 与输入哈希核对
├── messages-only/
│   ├── trajectories/token-plan/<日期>/<两位哈希>/<模型>/<原文件名>
│   ├── manifest.json                # 实际保留文件及其字节和哈希
│   ├── retained-input.jsonl          # 全部保留文件的共享路径
│   ├── decisions/                   # 每条输入的保留／合并决定
│   └── report.json
├── audit/                           # 全部输出字节和最终对应关系核验
├── recheck/                         # 全部保留文件再次比较，新增合并 0
├── merge-relations.json              # 每条合并轨迹 → 最终保留轨迹
├── summary.json                     # 总数、模型目录分布及完整核验结果
└── control-*/                       # 源码快照、提交参数、日志和成功回执
```
