# 核验方法

对小规模保留结果可使用 `--review-pairs --extra-information messages_only` 做全部两两诊断，限制为 2–128 条、总大小不超过 256 MiB。该模式通过 Cluster 读取实际共享文件并核验哈希，比较所有配对，独立验证现有前缀规则，并检查换行、尾部空白、工具参数 JSON 编码、开头指令、后台通知和脱敏占位符等候选差异。候选探针只用于定位问题，不删除轨迹或修改去重规则；尤其脱敏占位符不能证明被替换的原始值。每对的完整位置差异、消息对齐和有限文本摘录保存在仓库外的 `pairs/`，不得把其中的原始文本提交到 Git。

主任务完成后，核验作业仍通过同一 Cluster 队列运行，输出到新的共享目录。不能用 `spark-submit` 返回 0、文件数量减少或者第二遍不再删除，单独证明去重正确。

```bash
.venv/bin/python scripts/submit_cluster.py \
  --config /share/project/private/cluster.json \
  --control /share/project/runs/control-audit-01 \
  --output /share/project/runs/audit-01 \
  --audit-run /share/project/runs/data-01 \
  --executors 16 --executor-cores 8 --partitions 1024 --execute
```

核验覆盖：

1. 原输入清单、决策清单完整对应，无重复记录；保留清单和 manifest 与决策相符。
2. 每个输出文件都按原始清单核对字节数和 SHA256；检查真正的输出文件，禁止从 S3 或下载缓存代替不存在的输出。
3. 每条删除记录都指向实际保留的最终文件，不得指向另一个已经删除的中间文件。
4. 独立的 `raw_covers` 从完整 JSON 对象逐项比较身份、消息、工具引用及所选的附加信息约束。默认只检查工具定义；`preserve_all` 才检查其他附加信息，`messages_only` 不检查附加信息。它使用原始 ID 的双射关系检查绑定，不使用生产扫描器的排序、消息哈希或编号投影来证明删除。
5. 对每条删除关系逐项关闭鲁棒规则，统计当前最终见证文件依赖哪些规则。`strict_witness` 仅表示这条最终对应关系在关闭所有鲁棒规则后仍成立，不能冒充单独运行旧算法得到的全量去重结果。

随后将 `data-01/retained-input.jsonl` 作为新的 `--input-plan` 再跑一次，检查是否仍存在符合当前规则的可合并轨迹。输出写到新目录，不能复用原输出。

TB 规模的第二遍可加 `--check-only --no-input-cache`。它仍重新读取并校验全部输入、重新排序并执行完整前缀比较，保存 `decisions/`、`report.json` 和完成标记；省去再次复制保留文件，不生成轨迹目录或 manifest。报告的 `check_only=true` 标明这一模式，不提供 `output_bytes`。首次实际输出仍须通过上面的独立全量核验；复查目录只用于查看是否还有可合并记录。

对 `check-only` 结果也可以使用同一 `--audit-run` 命令独立核验。此时先检查原输入清单哈希及每条决策的完整对应关系，再从原始输入读取每个保留文件和每条删除关系的短／长文件，逐个验证字节哈希和实际消息覆盖。每个删除目标必须确实是保留决策。报告使用 `audit_mode=input_decisions`、`retained_input_hash_verified`，不声称已经写出或核验了输出文件，也不执行基于输出目录的近邻分析。`messages_only` 的核验还记录当前删除关系中有多少需要取消工具定义约束；这是关系计数，净增合并量仍需与同一完整输入的 `tools_only` 独立运行比较。

独立扫描保留结果中的近似前缀，还需要区分实际用户分支、工具参数或结果变化、工具定义缺失等。默认策略忽略单独存放的压缩信息，但消息中出现的新摘要或压缩指令仍会参与比较。第二遍删除为 0 只证明当前规则下的幂等性，不证明世界上不存在其他可以安全归一化的差异。

核验作业还逐个读取保留文件，检查原排序分区中相邻记录：短记录至少 4 条消息、共同前缀达到短记录的 80% 时，记录差异类别，并在 `near-prefix-samples/` 保存有界样例。统计单位是候选对，不是去重后的轨迹数；样例中的消息是用于比较的投影片段。该方法用于寻找待人工核对的边界，不据此自动删除记录，也不能发现所有首两条消息不同的情况。

## 开头指令差异的补充检查

对实际保留结果使用 `--review-headers`，可检查另一类候选：同一原比较组内，去掉开头连续的 system/developer 消息后，至少四条正文消息完全一致，工具定义及其他所选保护条件一致，但开头指令不同。

```bash
.venv/bin/python scripts/submit_cluster.py \
  --config /share/project/private/cluster.json \
  --control /share/project/runs/control-headers-01 \
  --output /share/project/runs/headers-01 \
  --input-plan /share/project/runs/data-01/retained-input.jsonl \
  --review-headers --no-input-cache \
  --executors 8 --executor-cores 8 --partitions 1024 --execute
```

本检查重新读取清单中的每个已落盘文件，校验原字节和 SHA256。哈希只挑候选，再从每个候选组中挑两个不同开头的代表文件，重新读取实际文件、精确核对正文及工具定义。每组最多保留两个代表，不把整个大组装入内存。

`candidates/` 保存验证后的候选组及原文件引用，`sample.json` 保存按固定哈希选出的最多 300 组，`report.json` 给出组数与涉及条数。报告还检查两个代表的开头差异能否仅由尾部 ASCII 空白、CRLF 或首行 billing header 解释；这些标记可以重叠。

候选组数和涉及条数都不是可删除数量。真实指令变化也会进入候选，必须检查实际含义才能决定是否增加规则。此检查只写独立核验材料，不删除或复制轨迹，不放宽正式去重策略；它只覆盖正文完全一致的情况，不穷尽长短前缀的开头差异。

测试比例以实际进入本次测试的记录为分母。每日渠道采用固定文件名家族抽样，保留选中的完整家族；没有真实 session 的哈希文件名不能证明覆盖了同一隐含会话的全部文件，因此不能把样本比例直接推广到渠道全量。
