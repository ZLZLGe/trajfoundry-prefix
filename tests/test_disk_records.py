import json
import sqlite3

import pytest

from trajectory_prefix.disk_records import DiskRecords


def test_streamed_records_have_stable_views_and_unique_identity(tmp_path):
    parts = tmp_path / "parts"
    parts.mkdir()
    data = [{"path": "a", "trajectory_id": "1", "status": "kept"},
            {"path": "b", "trajectory_id": "2", "status": "removed"}]
    (parts / "part-0").write_text("\n".join(map(json.dumps, data)))
    records = DiskRecords.from_parts(tmp_path / "rows.sqlite", parts)
    assert list(records) == ["a", "b"]
    assert list(records.values()) == data
    assert len(records.view()) == 2
    assert list(records.view("kept")) == data[:1]
    assert records["b"] == data[1]
    with pytest.raises(sqlite3.IntegrityError):
        records.add("c", data[0], "1")
