import orjson
from survey_lineage_sets import compare, inventory


def run(groups):
    rows = [{'trajectory_id': f'r{i}', 'origin_count': len(g),
             'origins': [{'source_ref': p, 'sha256': h} for p, h in g]} for i, g in enumerate(groups)]
    ids, sets, counts, _ = inventory(orjson.dumps(r) + b'\n' for r in rows)
    witnesses = []
    return compare(ids, sets, counts, witnesses.append), witnesses


def test_equal_sets_strict_supersets_and_shared_only_across_different_records():
    a, b, c, d, e = [(str(i), 'hash') for i in range(5)]
    counts, rows = run([[a], [a], [a, b], [b, c], [c, d], [e]])
    assert counts['records_with_equal_set_peer'] == 2
    assert counts['records_with_equal_and_strict_peer'] == counts['records_covered_by_one_other'] == 2
    assert counts['records_shared_only_across_multiple_peers'] == 2
    assert counts['records_with_an_exclusive_origin'] == 2
    assert sorted(r['trajectory_id'] for r in rows if r['strict_superset_peer'] == 'r2') == ['r0', 'r1']


def test_origin_requires_both_path_and_hash_and_zero_is_explicit():
    counts, rows = run([[('x', 'one')], [('x', 'two')], [('y', 'one')]])
    assert counts['records_with_only_exclusive_origins'] == 3
    assert 'records_covered_by_one_other' in counts and counts['records_covered_by_one_other'] == 0
    assert not rows
