import hashlib
import io

import orjson
import pytest
from botocore.response import StreamingBody
from verify_published_identifiers import verify as verify_identifiers
from verify_published_membership import consume_plan, read_lineage


def test_plan_matches_keys_and_checksums_even_when_counts_agree(tmp_path):
    row = {'path': 'a.jsonl', 'bytes': 5, 'sha256': 'abc'}
    manifest = [row, {'path': 'lineage.jsonl', 'bytes': 23, 'sha256': 'def'}]
    p = tmp_path / 'plan'
    p.write_bytes(orjson.dumps(row) + b'\n')
    assert consume_plan(manifest, p, [dict(removed={'path': 'b.jsonl'}, retained=row)])[:2] == (1, 5)
    with pytest.raises(AssertionError):
        consume_plan([*manifest, row], p, [])
    with pytest.raises(AssertionError):
        consume_plan([{**row, 'sha256': 'abc123'}, manifest[1]], p, [])
    with pytest.raises(KeyError):
        consume_plan([{**row, 'path': 'b.jsonl'}, manifest[1]], p, [])


def test_anchors_require_together_on_the_verified_id_and_allow_reused_origins():
    a, b = ('row-a', '01' * 32), ('row-b', '02' * 32)

    def validate(rows):
        payload = b''.join(orjson.dumps({'trajectory_id': name, 'origin_count': len(origins),
                           'origins': [{'source_ref': ref, 'sha256': sha} for ref, sha in origins]})
                           + b'\n' for name, origins in rows)
        return read_lineage(StreamingBody(io.BytesIO(payload), len(payload)),
                            (len(payload), hashlib.sha256(payload).hexdigest()), {'pair': ('target', {a, b})})

    r = validate([('another', [a]), ('target', [a, b])])
    assert r['distinct_origins'] == 2 and r['origin_instances'] == 3
    assert r['located_anchors'] == {'pair': True} and r['other_rows_sharing_anchor_origins'] == {'pair': 1}
    with pytest.raises(AssertionError):
        validate([('target', [a]), ('other', [b])])
    with pytest.raises(AssertionError):
        validate([('wrong-id', [a, b])])
    with pytest.raises(AssertionError):
        validate([('target', [a, b]), ('target', [a, b])])


def test_id_set_is_checked_when_lineage_row_count_is_unchanged(tmp_path):
    root = tmp_path / 'channel'
    pub = root / 'published'
    inspection = pub / 'source-inspection-test'
    (pub / 'stages').mkdir(parents=True)
    inspection.mkdir()
    (inspection / '_SUCCESS').touch()
    (root / 'refined').mkdir()
    ref = {'path': 'one', 'bytes': 7, 'sha256': '1' * 64}
    plan = orjson.dumps(ref) + b'\n'
    (root / 'refined/final-input.jsonl').write_bytes(plan)
    (pub / 'report.json').write_bytes(orjson.dumps({'publication_id': 'p'}))
    (pub / 'stages/source-inspection.json').write_bytes(
        orjson.dumps({'publication_id': 'p', 'path': str(inspection), 'rows': 1}))
    (inspection / 'part-00000').write_bytes(orjson.dumps({**ref, 'trajectory_id': 'one-id',
                                               'final_path': 'one', 'status': 'kept'}) + b'\n')
    evidence = {'passed': True, 'channel': 'channel', 'counts': {'A': 1, 'C': 1}, 'target_manifest_sha256': '',
                'ordinary_rechecked_plan_sha256': hashlib.sha256(plan).hexdigest()}

    def assert_result(identifier):
        payload = orjson.dumps({'trajectory_id': identifier}) + b'\n'
        (pub / 'lineage.jsonl').write_bytes(payload)
        return verify_identifiers(root, {**evidence, 'lineage': {'sha256': hashlib.sha256(payload).hexdigest()}})

    assert assert_result('one-id')['distinct_retained_files_with_unique_lineage_id'] == 1
    with pytest.raises(KeyError):
        assert_result('another-id')
