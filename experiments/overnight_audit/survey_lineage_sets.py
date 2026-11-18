"""Check exact origin-set overlap across an entire verified publication lineage."""

import argparse
import hashlib
import os
from array import array
from collections import Counter, defaultdict
from pathlib import Path

import orjson


def inventory(lines):
    lookup, frequency, ids, groups, seen = {}, array('I'), [], [], set()
    size, occurrences, digest = 0, 0, hashlib.sha256()
    for line in lines:
        size += len(line)
        digest.update(line)
        row = orjson.loads(line)
        identifier = row['trajectory_id']
        assert identifier not in seen
        seen.add(identifier)
        origins = []
        for origin in row['origins']:
            key = (origin['source_ref'], origin['sha256'])
            index = lookup.get(key)
            if index is None:
                index = len(frequency)
                lookup[key] = index
                frequency.append(0)
            frequency[index] += 1
            origins.append(index)
        assert origins and len(set(origins)) == len(origins) == row['origin_count']
        occurrences += len(origins)
        ids.append(identifier)
        groups.append(tuple(sorted(origins)))
    receipt = {'bytes': size, 'sha256': digest.hexdigest(), 'rows': len(ids),
               'origin_instances': occurrences, 'distinct_origins': len(frequency)}
    return ids, groups, frequency, receipt


def compare(ids, origins, frequency, write):
    counts = Counter(dict.fromkeys((
        'records_with_an_exclusive_origin', 'records_with_a_shared_origin', 'records_with_only_exclusive_origins',
        'records_with_all_origins_shared', 'records_with_equal_set_peer', 'records_with_strict_superset_peer',
        'records_with_equal_and_strict_peer', 'records_covered_by_one_other',
        'records_shared_only_across_multiple_peers',
    ), 0))
    # Only rows whose every origin occurs at least twice could be contained.
    candidates = defaultdict(list)
    for i, group in enumerate(origins):
        exclusive = sum(frequency[k] == 1 for k in group)
        counts['records_with_an_exclusive_origin'] += bool(exclusive)
        counts['records_with_a_shared_origin'] += exclusive < len(group)
        counts['records_with_only_exclusive_origins'] += exclusive == len(group)
        if not exclusive:
            candidates[group].append(i)

    def anchor(group):
        return min(group, key=lambda k: (frequency[k], k))

    postings = {anchor(group): [] for group in candidates}
    if postings:
        for i, group in enumerate(origins):
            for k in group:
                if k in postings:
                    postings[k].append(i)
    for group, members in candidates.items():
        needed = set(group)
        equal = len(members) > 1
        strict = next((i for i in postings[anchor(group)]
                       if len(origins[i]) > len(group) and needed.issubset(origins[i])), None)
        counts['records_with_all_origins_shared'] += len(members)
        counts['records_with_equal_set_peer'] += len(members) if equal else 0
        counts['records_with_strict_superset_peer'] += len(members) if strict is not None else 0
        counts['records_with_equal_and_strict_peer'] += len(members) if equal and strict is not None else 0
        counts['records_covered_by_one_other'] += len(members) if equal or strict is not None else 0
        counts['records_shared_only_across_multiple_peers'] += len(members) if not equal and strict is None else 0
        for j, i in enumerate(members):
            other = members[0] if j else members[-1]
            write({'trajectory_id': ids[i], 'origins': len(group),
                   'equal_set_peer': ids[other] if equal else None,
                   'strict_superset_peer': ids[strict] if strict is not None else None,
                   'superset_size': len(origins[strict]) if strict is not None else None})
    assert counts['records_with_an_exclusive_origin'] + counts['records_with_all_origins_shared'] == len(ids)
    assert (counts['records_covered_by_one_other'] + counts['records_shared_only_across_multiple_peers']
            == counts['records_with_all_origins_shared'])
    return counts


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--membership', type=Path, required=True)
    parser.add_argument('--lineage', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    os.umask(0o077)
    membership = orjson.loads(args.membership.read_bytes())
    assert membership['passed'] and membership['all_published_member_references_match']
    expected = membership['lineage']
    with args.lineage.open('rb') as stream:
        ids, groups, frequency, observed = inventory(stream)
    assert observed == {key: expected[key] for key in observed}
    assert observed['rows'] == membership['counts']['C']
    args.output.mkdir(parents=True, exist_ok=False)
    with (args.output / 'all-shared-records.jsonl').open('wb') as output:
        counts = compare(ids, groups, frequency, lambda row: output.write(orjson.dumps(row) + b'\n'))
    freq = Counter(frequency)
    counts['origins_shared_across_records'] = sum(n for v, n in freq.items() if v > 1)
    counts['origins_on_only_one_record'] = freq[1]
    report = {'passed': True, 'channel': membership['channel'], 'output_uri': membership['output'],
              'lineage': observed, 'trajectory_counts': counts, 'origin_frequency_max': max(freq),
              'definition': 'An origin is the exact source_ref and SHA-256 pair. Compare all records within '
                            'this published date. For a strict or equal set give one direct witness. A shared '
                            'source alone does not identify an equivalent conversation.'}
    (args.output / 'report.json').write_bytes(orjson.dumps(report, option=orjson.OPT_INDENT_2) + b'\n')
    (args.output / 'SUCCEEDED').write_text('All origin sets compared on the verified published lineage.\n')
    print(orjson.dumps(report).decode(), flush=True)


if __name__ == '__main__':
    main()
