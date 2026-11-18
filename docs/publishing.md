# 发布已核验的标准化去重结果

`scripts/submit_publication.py` 是单独的 S3 发布入口。原来的比较命令和 `Reader` 仍只读 S3。发布入口复用已成功的全量去重、独立审计和保留集第二遍检查，要求第二遍新增合并为零。

业务处理必须提交 Spark Cluster。提交器复制当前源码和指定的 TrajFoundry 校验源码到控制目录，并记录文件哈希；Driver 和 Executor 使用这一快照。凭据只有仓库外文件路径，不写入配置值、日志或输出。

```bash
.venv/bin/python scripts/submit_publication.py \
  --config /share/project/private/cluster.json \
  --publication-config /share/project/runs/publication.json \
  --validation-source /share/project/releases/validated/src/trajfoundry \
  --control /share/project/runs/publish-control \
  --output /share/project/runs/publish-receipts \
  --executors 16 --executor-cores 8 --partitions 512 --execute
```

发布配置包含 `source_uri`（标准化日期分区）、`normalized_uri`（新日期分区）、`selection_base_uri`、`selection_version`、`models`、`dataset`、`date`、三个 `min_*` 阈值，以及已核验的 `source_run`、`audit_report`、`recheck_report`、`policy`、`source_manifest_sha256`、`input_plan_sha256`、`decision_files_sha256`。S3 设置为 `s3_endpoint`、`s3_region`、`s3_credentials_file`，必须与 Cluster 配置相同。决策文件摘要是排序后的 `{part文件名: SHA256}` 经本模块 `encoded` 序列化后的 SHA256。

`publish_selection` 默认为 `true`，同时发布标准化和模型筛选结果。设为 `false` 时，只处理标准化目标。此时可以省略 `selection_base_uri`、`selection_version`、三个 `min_*` 参数和 `models`。显式提供 `models` 时仍逐条核对模型归属，发布回执按实际保留轨迹统计模型分布。

`source_inventory_receipt` 指向全前缀探查时冻结的清单回执，`source_inventory_sha256` 固定其内容。回执须包含源 URI、manifest 和 input plan 摘要、轨迹数与字节数、`complete_requested_prefix=true`、`non_trajectory_files`（path、bytes）。源目录中未列入 manifest 的旧诊断文件也必须与该清单完全一致；未知文件或大小变化均失败。诊断文件不复制到新版本。

## 普通前缀发布

发布只接受普通前缀决策，省略 `comparison_mode` 或显式指定 `prefix` 均可。旧脱敏配置和带 `mask_version` 的源回执会在提交前被拒绝。独立审计须来自本次去重运行，保留集复查清单中的每个轨迹身份、字节数、SHA256 须与本次保留决策一致。发布端从源文件核验每条删除记录与其最终代表的普通前缀关系，再开始复制。

## 数据与来源

- 整套源轨迹按冻结清单校验字节和 SHA256。保留轨迹使用与源版本匹配的契约重新校验文件名、统一字段和派生指标。
- 轨迹原文件直接复制，每个目标文件完整读取并核验 SHA256。主轨迹、子轨迹、工具定义、reasoning 和已有质量字段均不改写。
- 来源沿最终保留关系合并。同一原始来源只保留一次；原始哈希或采集时间冲突会失败。重复的质量证据取并集，disposition 保留已有证据中最严重的值。
- 新 lineage 的代表 ID 与文件内容对应，原始来源集合必须保持一致。标准化 manifest 保持原契约，原始输入和失败统计沿用原始采集口径；输出数量、字节、来源重复计数重新计算。`duplicate_trajectories` 是所有 `origin_count - 1` 的和，不是本次删除文件数。
- 筛选以根轨迹的 `total_tokens`、`total_tool_calls`、`total_rounds` 为准，三个阈值都包含等于。`total_rounds` 是主 messages 的 assistant 数。质量标签不参与筛选。

TrustCyberData 的完整归档使用 `archive_rows=true` 和该归档版本的校验源码。lineage 以原文件名和行的字节偏移共同标识来源，每个偏移的哈希都须保持一致。归档 manifest 中的输入文件、原始行数、原始成功数和原始失败数沿用采集口径。轨迹数、消息与工具统计、保留轨迹的改写计数按新版本重算。`duplicate_rows` 是已由代表轨迹覆盖的其余输入行数，含本轮前缀覆盖的行。

## 目标与完成标记

标准化目录为 `<新版本>/dt=日期/`，只含原名轨迹、`lineage.jsonl`、标准化 `manifest.json`。筛选目录为 `<筛选前缀>/<模型>/<版本>/dt=日期/`，模型版本根目录只额外放 9 字段 manifest：`model`、`source`、`min_tokens`、`min_tool_calls`、`min_assistant_steps`、`date_start`、`date_end`、`trajectory_count`、`total_bytes`。不输出分片索引。

所有目标版本首次运行必须为空。清单最后写入；未出现 manifest 的目标不能视为完成。部分模型已完成、其他模型失败时，完整交付仍以共享目录的成功回执为准。

S3 对象 metadata 记录源 SHA256 和本次 publication ID。失败后用新的控制目录、同一配置和输出目录，加 `--resume`；只复用本次拥有且内容一致的对象。旧版本、不同运行的同名对象和不同内容均不会覆盖。已完成复制阶段在重试时也重新读取目标内容核验；失败目录和日志保留。

共享输出保存冻结清单、逐文件检查与复制回执、候选 manifest、最终 `report.json` 和 `SUCCEEDED`。`control/final.json` 另外确认实际 Driver 成功以及队列一致，不能只凭 spark-submit 返回码判断完成。
