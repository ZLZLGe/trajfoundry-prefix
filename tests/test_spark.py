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
    checked = tmp_path / "checked"
    comparison = run(spark, p, checked, partitions=2, check_only=True)
    assert comparison["input_count"] == 3 and comparison["removed_count"] == 2
    assert comparison["check_only"] is True and "output_bytes" not in comparison
    assert not (checked / "trajectories").exists() and not (checked / "manifest.json").exists()
    decisions = [
        json.loads(line)
        for part in (checked / "decisions").glob("part-*")
        for line in part.read_text().splitlines()
    ]
    assert {d["record"]["path"] for d in decisions} == {e["path"] for e in plan}
    assert sum(d["status"] == "removed" for d in decisions) == 2
    for e in plan:
        assert hashlib.sha256(Path(e["local_path"]).read_bytes()).hexdigest() == e["sha256"]


def test_header_review_verifies_actual_bodies_without_publishing_trajectories(spark, tmp_path):
    from trajectory_prefix.header_review import run as review_headers
    from trajectory_prefix.projection import Policy

    plan = []
    for index, header in enumerate(["instruction A", "instruction B", "instruction C"]):
        body = [
            {"role": "system", "content": header},
            {"role": "user", "content": "question" if index < 2 else "different question"},
            {"role": "assistant", "content": "answer"},
            {"role": "user", "content": "continue"},
            {"role": "assistant", "content": "done"},
        ]
        data = json.dumps(
            {"model": "m", "metadata": {"session_id": "s"}, "messages": body}
        ).encode()
        path = tmp_path / f"input-{index}.jsonl"
        path.write_bytes(data)
        plan.append(
            {
                "dataset": "test",
                "date": "2026-10-10",
                "path": path.name,
                "local_path": str(path),
                "bytes": len(data),
                "sha256": hashlib.sha256(data).hexdigest(),
            }
        )
    input_plan = tmp_path / "plan.jsonl"
    input_plan.write_text("".join(json.dumps(row) + "\n" for row in plan))
    output = tmp_path / "headers"
    result = review_headers(spark, input_plan, output, partitions=2, policy=Policy())
    assert result["input_count"] == 3 and result["eligible_count"] == 3
    assert result["candidate_groups"] == 1 and result["records_in_candidate_groups"] == 2
    sample = json.loads((output / "sample.json").read_text())
    assert len(sample) == 1 and sample[0]["verified_equal_body_pair"]
    assert not any(sample[0]["equal_after_header_transform"].values())
    assert not (output / "trajectories").exists() and not (output / "manifest.json").exists()
    for row in plan:
        assert hashlib.sha256(Path(row["local_path"]).read_bytes()).hexdigest() == row["sha256"]
