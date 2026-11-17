"""Check witness completeness where equal-label neighbors hide another label."""
import itertools
import random

import run_harness_probe as probe


def test_witness_scan_matches_exhaustive_search():
    rng = random.Random(20261118)
    for _ in range(100):
        rows = []
        for i in range(40):
            scope = rng.choice([b'a', b'b'])
            vector = bytes(rng.choices(range(1, 4), k=rng.randint(1, 5)))
            rows.append(((scope, vector, str(i)), {'ref': {'id': str(i)}, 'harness': rng.choice(['x', 'y', 'z'])}))
        expected = {a[1]['ref']['id'] for a, b in itertools.permutations(rows, 2)
                    if a[0][0] == b[0][0] and a[1]['harness'] != b[1]['harness']
                    and b[0][1].startswith(a[0][1])}
        actual = list(probe.candidates(sorted(rows)))
        assert {r['short']['id'] for r in actual} == expected
        assert len(actual) == len(expected)
        lookup = {r[1]['ref']['id']: r for r in rows}
        for match in actual:
            a, b = [lookup[match[k]['id']] for k in ['short', 'long']]
            assert a[0][0] == b[0][0]
            assert b[0][1].startswith(a[0][1])
            assert a[1]['harness'] != b[1]['harness']
