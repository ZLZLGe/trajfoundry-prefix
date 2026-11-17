import json
from io import StringIO

import pytest

from trajectory_prefix.manifest_stream import ManifestStream


@pytest.mark.parametrize("chunk", [1, 3, 31, 1024])
def test_stream_manifest_across_arbitrary_boundaries(chunk):
    value = {"before": 123456, "files": [{"path": '中文\\".jsonl', "bytes": 123456}],
             "after": {"nested": [True, None, "line\ntext"]}}
    reader = ManifestStream(StringIO(json.dumps(value, ensure_ascii=False)), chunk)
    assert list(reader.files()) == value["files"]
    assert reader.metadata == {k: v for k, v in value.items() if k != "files"}


@pytest.mark.parametrize("text", ['{"files":[]', '{"files":[],"files":[]}',
                                  '{"files":[]} trailing', '{"files":[4]}'])
def test_reject_incomplete_or_ambiguous_manifest(text):
    with pytest.raises(ValueError):
        list(ManifestStream(StringIO(text), 3).files())
