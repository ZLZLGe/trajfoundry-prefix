import hashlib

import pytest

from trajectory_prefix.storage import Reader, StorageConfig, atomic_bytes, reference


@pytest.mark.parametrize("path", ["../secret", "/absolute", "x//y", "x/./y", "x\\y"])
def test_unsafe_input_paths_rejected(path):
    with pytest.raises(ValueError):
        reference({"dataset": "d", "date": "date", "path": path, "sha256": "a" * 64, "bytes": 1})


def test_stale_hash_and_output_conflict_are_rejected(tmp_path):
    path = tmp_path / "input"
    path.write_bytes(b"old")
    entry = {"id": "x", "local_path": str(path), "bytes": 3, "sha256": "a" * 64}
    with pytest.raises(ValueError):
        Reader(StorageConfig()).read(entry)
    target = tmp_path / "target"
    target.write_bytes(b"old")
    with pytest.raises(FileExistsError):
        atomic_bytes(target, b"new", hashlib.sha256(b"new").hexdigest())
    assert target.read_bytes() == b"old"
