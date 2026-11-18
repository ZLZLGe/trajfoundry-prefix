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


@pytest.mark.parametrize(("receipts", "valid"), [
    ([("c", "3"), ("a", "1")], True),
    ([("a", "1")], False),
    ([("a", "1"), ("a", "1")], False),
    ([("c", "1"), ("a", "3")], False),
    ([("c", "3"), ("b", "2")], False),
    ([("c", "3"), ("x", "X")], False),
])
def test_copy_receipts_cover_each_retained_path_and_id_once(tmp_path, receipts, valid):
    records = DiskRecords(tmp_path / "inspection.sqlite")
    try:
        for path, identifier, status in [("a", "1", "kept"), ("b", "2", "removed"), ("c", "3", "kept")]:
            records.add(path, {"status": status}, identifier)
        parts = tmp_path / "copies"
        parts.mkdir()
        for i, (path, identifier) in enumerate(receipts):
            row = {"path": path, "trajectory_id": identifier}
            (parts / f"part-{i}").write_text("\n" + json.dumps(row) + "\n")
        if valid:
            assert records.verify_retained_copies(parts) == 2
            assert records.verify_retained_copies(parts) == 2
        else:
            with pytest.raises(ValueError, match="Normalized copy receipt"):
                records.verify_retained_copies(parts)
    finally:
        records.db.close()


def test_empty_copy_receipts(tmp_path):
    records = DiskRecords(tmp_path / "inspection.sqlite")
    try:
        (tmp_path / "part-0").touch()
        assert records.verify_retained_copies(tmp_path) == 0
    finally:
        records.db.close()


def test_identifier_count_matches_the_set_of_inspected_ids(tmp_path):
    records = DiskRecords(tmp_path / "inspection.sqlite")
    try:
        identifiers = []
        for i, identifier in enumerate(["甲", None, "1", None]):
            assert records.distinct_identifier_count() == len(set(identifiers))
            records.add(str(i), {"trajectory_id": identifier}, identifier)
            identifiers.append(identifier)
        assert records.distinct_identifier_count() == len(set(identifiers))
    finally:
        records.db.close()


def test_retained_identity_cursor_matches_python_path_order(tmp_path):
    rows = [("乙", "3", "kept"), ("a", "1", "kept"), ("b", "4", "removed"), ("é", "2", "kept")]
    records = DiskRecords(tmp_path / "index.sqlite")
    try:
        for path, identifier, status in rows:
            records.add(path, {"status": status}, identifier)
        assert list(records.retained_identifiers()) == sorted((p, i) for p, i, s in rows if s == "kept")
    finally:
        records.db.close()
