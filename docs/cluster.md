# Cluster 提交与输出

这里的业务验证由当前机器运行 `spark-submit --deploy-mode cluster`，Driver 和 Executor 均在 Kubernetes 中运行，使用配置中的用户队列。`local[2]` 只用于小型单元／集成测试。

每次提交复制 `src/` 和 `scripts/` 到独立控制目录，记录每个文件的 SHA256。运行期间修改开发目录不会改变已经提交的作业。无需导入、安装或修改旧 TrajFoundry 生产项目。

## 环境配置

配置 JSON 放在仓库外，包含以下键。认证参数均为文件路径，不能填写密钥值。

| 键 | 含义 |
|---|---|
| `master`, `namespace`, `queue` | Kubernetes HTTPS API、命名空间、用户队列；队列同时用于 Driver 和 Executor 标签 |
| `service_account`, `ca_cert`, `oauth_token_file` | Pod ServiceAccount、提交侧 CA 和 Token 文件；保留 TLS 校验 |
| `spark_submit`, `submit_java_home` | 提交机器上的 Spark 和 Java 路径 |
| `image`, `pod_template`, `container_name` | Spark 镜像、带共享卷的 JSON Pod 模板、应用容器名 |
| `shared_root` | Driver、Executor 和提交机都能访问的共享目录根路径 |
| `uid`, `gid`, `arm_python` | Pod 身份和 ARM Python 3.11；环境需有 `orjson`、`boto3` |
| `s3_endpoint`, `s3_region`, `s3_credentials_file` | S3 只读输入配置；凭据文件由 Pod 用户持有，权限 0400 或 0600 |
| `extra_listener` | 可选，平台的 Spark Listener 类名 |

Executor 临时文件与 SQLite 都使用 Pod 本地磁盘；代码、输入清单和最终结果使用共享目录。输入可由 Executor 直接从 S3 获取，按清单的字节数和 SHA256 校验，并缓存在当前 Pod 的临时目录中。只允许 S3 GetObject，没有 S3 写入操作。

完整日期的数据可能达到 TB 规模。可加 `--no-input-cache`，使各阶段重新读取并校验输入，避免所有读过的文件长期保留在各 Pod 的临时磁盘中。共享文件系统中的输入和最终输出仍正常读写，SQLite 和 Spark shuffle 仍使用临时磁盘；此选项不改变去重规则。

## 提交命令

先验证共享目录、ARM 环境、字节保留、两次去重；这里的路径仅为示例，替换为自己的共享目录。

```bash
.venv/bin/python scripts/submit_cluster.py \
  --config /share/project/private/cluster.json \
  --control /share/project/runs/control-probe-01 \
  --output /share/project/runs/probe-01 \
  --probe --executors 2 --executor-cores 1 --execute
```

真实数据运行：

```bash
.venv/bin/python scripts/submit_cluster.py \
  --config /share/project/private/cluster.json \
  --control /share/project/runs/control-data-01 \
  --output /share/project/runs/data-01 \
  --input-plan /share/project/input.jsonl \
  --extra-information tools_only \
  --executors 16 --executor-cores 8 --partitions 1024 --execute
```

不加 `--execute` 只生成计划和代码快照；真正执行请用新的控制和输出目录重新提交。程序拒绝复用已有输出目录，失败产物保留供排查。提交脚本会检查 Driver Pod 是否 Succeeded，不能只根据 `spark-submit` 的退出码判断任务成功。

`tools_only` 为默认值，附加信息只检查工具定义。需要与旧的全部附加信息保护策略对比时，使用 `--extra-information preserve_all`。运行报告保存具体策略，核验程序按照该次运行的策略验证，不能混用。

`--check-only` 可用于对保留结果再比较一遍：仍处理输入清单中的每条完整轨迹，只写决策和报告，不再复制轨迹文件。它要求 `--input-plan`，不能与 `--probe` 或 `--audit-run` 组合。具体核验顺序见 [validation.md](validation.md)。

百万级文件建议加 `--output-layout hash2`，输出路径为 `trajectories/渠道/日期/两位哈希/原文件名`。每个日期最多 256 个子目录，避免所有 Executor 同时向单个目录创建小文件。哈希只决定文件位置，轨迹字节和原文件名不变；manifest 和保留输入清单指向真实路径，核验器兼容两种布局。默认 `flat` 保留原目录形式。

比较完成后会写入独立的 `decision-report.json`。若后面的文件写出被中断，可在新输出目录加 `--reuse-decisions /share/project/runs/data-01` 复用已完成的 `decisions/`，只重新写出结果。程序要求决策目录存在 `_SUCCESS`，输入清单路径和 SHA256、比较策略以及复制后的全量计数都相符。后续仍必须独立核验实际输出和每条删除依据。此模式不用于第二遍去重检查，`--check-only` 和 `--review-headers` 始终重新处理全部输入。

## 目录

```text
control-data-01/
  app/                       # 本次代码快照
  source-hashes.json          # 快照文件校验
  driver-pod.json
  executor-pod.json
  plan.json                  # 队列、命令、输入输出路径
  driver.log
  submit.log
  eventLog/
  final.json                 # Driver 与输出完成证据
data-01/
  trajectories/渠道/日期/原文件名
  manifest.json              # trajectory_count、total_bytes、files
  retained-input.jsonl       # 可直接用于第二遍检查的输入清单
  decisions/                 # 每条输入的保留／移除决定及最终见证文件
  report.json                # 计数、规则、Spark 版本
  package-*.zip              # Spark 自动分发的代码包，不是数据压缩包
  SUCCEEDED                  # 完成后才创建
```

`manifest.files` 每项只有 `path`、`bytes`、`sha256`。这些审计文件都独立于轨迹本身，轨迹字节不变。此 manifest 是新项目的输出清单；旧的 `run_s3_label_job` 还要求原标准化 manifest 和 lineage 契约，尚不能直接把此目录当作原调度节点输入。
