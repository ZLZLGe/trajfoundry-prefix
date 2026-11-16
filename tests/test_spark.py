import hashlib
import json
from pathlib import Path

import pytest

from trajectory_prefix.spark import run


@pytest.fixture(scope="module")
def spark():
    from pyspark.sql import SparkSession

    session = (
        SparkSession.builder.master("local[2]")
        .appName("prefix-tests")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


def test_spark_keeps_original_bytes_and_rechecks_output(spark, tmp_path):
    root = tmp_path / "inputs"
    root.mkdir()
    plan = []
    messages = [
        {"role": "user", "content": "inspect project"},
        {"role": "assistant", "content": "answer"},
    ]
    for i in range(3):
        node = {
            "messages": messages
            + (
                [{"role": "user", "content": "continue"}, {"role": "assistant", "content": "done"}]
                if i
                else []
            ),
            "metadata": {"session_id": "s", "user_id": "u"},
            "model": "m",
            "tools": [],
            "custom": {"keep": "unchanged"},
        }
        data = (json.dumps(node, indent=2) + "\n").encode()
        path = root / f"r{i}.jsonl"
        path.write_bytes(data)
        plan.append(
            {
                "dataset": "sample",
                "date": "2026-01-01",
                "path": path.name,
                "local_path": str(path),
                "sha256": hashlib.sha256(data).hexdigest(),
                "bytes": len(data),
            }
        )
    p = tmp_path / "plan.jsonl"
    p.write_text("".join(json.dumps(e) + "\n" for e in plan))
    output = tmp_path / "out"
    result = run(spark, p, output, partitions=3)
    assert result["input_count"] == 3 and result["kept_count"] == 1
    manifest = json.loads((output / "manifest.json").read_text())
    for e in manifest["files"]:
        body = (output / e["path"]).read_bytes()
        assert body in [Path(x["local_path"]).read_bytes() for x in plan]
    second = run(spark, output / "retained-input.jsonl", tmp_path / "again", partitions=2)
    assert second["removed_count"] == 0
