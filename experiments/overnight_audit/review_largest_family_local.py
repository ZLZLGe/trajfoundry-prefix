"""Index the largest filename family in a verified retained input plan for manual branch review."""

import argparse
import hashlib
import json
import os
import re
import threading
from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import audit_core as audit
import orjson

from trajectory_prefix.storage import Reader, StorageConfig

FAMILY = re.compile(r'^(.*)_sub_(\d+)\.jsonl$')


def records(path):
    with path.open('rb') as stream:
        for line in stream:
            record = orjson.loads(line)
            match = FAMILY.fullmatch(record['path'])
            if match is None:
                raise ValueError(f"Unrecognized family suffix in {record['path']!r}")
            yield record, match[1], int(match[2])


def closest_neighbors(index):
    # The longest prefix in a lexicographically sorted set occurs on an adjacent side.
    groups = defaultdict(list)
    for row in index:
        groups[row['scope_hash']].append(row)
    result = {}
    for group in groups.values():
        ordered = sorted(group, key=lambda row: (row['history'], row['sub']))
        for i, row in enumerate(ordered):
            options = []
            for neighbor in ordered[max(0, i - 1):i + 2]:
                if row is neighbor:
                    continue
                common = audit.common_prefix(row['history'], neighbor['history'])
                options.append((common, -abs(neighbor['sub'] - row['sub']), neighbor['sub']))
            if options:
                common, _, other = max(options)
                result[row['sub']] = (common, other)
    return result


def dump(path, value):
    path.write_bytes(orjson.dumps(value, option=orjson.OPT_INDENT_2) + b'\n')


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--refined', type=Path, required=True)
    parser.add_argument('--config', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    os.umask(0o077)
    args.output.mkdir(parents=True, exist_ok=False)
    check = json.loads((args.refined / 'ordinary-final-recheck/report.json').read_text())
    assert check['check_only'] and check['removed_count'] == 0
    plan = args.refined / 'final-input.jsonl'
    with plan.open('rb') as stream:
        plan_sha = hashlib.file_digest(stream, 'sha256').hexdigest()
    assert plan_sha == check['input_plan_sha256']
    sizes = Counter(family for _, family, _ in records(plan))
    assert sizes.total() == check['kept_count']
    name = min(sizes, key=lambda family: (-sizes[family], family))
    wanted = [(sub, record) for record, family, sub in records(plan) if family == name]
    assert len(wanted) == len({sub for sub, _ in wanted}) == sizes[name]
    config = json.loads(args.config.read_text())
    storage = StorageConfig(endpoint_url=config['s3_endpoint'], region=config['s3_region'],
                            credentials_file=config['s3_credentials_file'], cache_dir=str(args.output / 'sources'))
    local = threading.local()

    def read(item):
        if not hasattr(local, 'reader'):
            local.reader = Reader(storage)
        sub, ref = item
        node = local.reader.node(ref)
        messages, issues = audit.stable_messages(node)
        first = next((msg for msg in messages if msg['role'] == 'user'), None)
        md = node['metadata']
        return {
            'sub': sub, 'ref': ref, 'model': node['model'], 'harness': node['harness'],
            'scope_hash': hashlib.sha256(audit.dumps(audit.scope(node, ref))).hexdigest(),
            'session_matches_filename': md.get('session_id') == name, 'created_at': md.get('created_at'),
            'message_count': len(messages), 'raw_message_count': len(node['messages']),
            'first_user_content_hash': hashlib.sha256(first['content'].encode()).hexdigest() if first else None,
            'tool_call_count': sum(len(msg.get('tool_calls') or []) for msg in node['messages']),
            'roles': [msg['role'] for msg in messages],
            'history': b''.join(audit.digest(audit.dumps(msg)) for msg in messages),
            'binding_issues': issues,
        }

    with ThreadPoolExecutor(max_workers=6) as pool:
        index = list(pool.map(read, sorted(wanted)))
    nearest = closest_neighbors(index)
    exact, shorter_prefix, longer_cover, diverged = 0, 0, 0, 0
    by_sub = {row['sub']: row for row in index}
    role_pairs = Counter()
    with (args.output / 'family.jsonl').open('wb') as stream:
        for row in index:
            sub = row['sub']
            record = {k: v for k, v in row.items() if k not in {'history', 'roles'}}
            if sub in nearest:
                common, other = nearest[sub]
                other_length = by_sub[other]['message_count']
                if common == row['message_count'] == other_length:
                    exact += 1
                elif common == row['message_count']:
                    shorter_prefix += 1
                elif common == other_length:
                    longer_cover += 1
                else:
                    diverged += 1
                record.update(closest_sub=other, shared_visible_messages=common)
                if common < min(row['message_count'], other_length):
                    roles = sorted([row['roles'][common], by_sub[other]['roles'][common]])
                    role_pairs['/'.join(roles)] += 1
                    record['first_differing_roles'] = roles
            stream.write(orjson.dumps(record) + b'\n')
    user_counts = Counter(r['first_user_content_hash'] for r in index)
    objects = {r['ref']['sha256']: r['ref']['bytes'] for r in index}
    summary = {
        'passed': True, 'local_only': True, 'input_plan_sha256': plan_sha,
        'retained_plan_count': sizes.total(), 'filename_family_count': len(sizes),
        'repeated_filename_families': sum(1 for v in sizes.values() if v > 1),
        'repeated_with_no_session_filename_prefix': sum(1 for k, v in sizes.items()
                                                       if v > 1 and k.startswith('no_session_id_')),
        'family': name, 'family_count': len(index), 'sub_range': [min(by_sub), max(by_sub)],
        'distinct_source_objects_verified': len(objects), 'distinct_source_bytes_verified': sum(objects.values()),
        'same_session_as_filename': sum(row['session_matches_filename'] for row in index),
        'scope_count': len({r['scope_hash'] for r in index}),
        'models': dict(Counter(r['model'] for r in index)), 'harnesses': dict(Counter(r['harness'] for r in index)),
        'records_without_user': user_counts[None],
        'distinct_first_user_texts': len(set(user_counts) - {None}),
        'most_frequent_first_user_text_group_sizes': [n for _, n in user_counts.most_common(12)],
        'single_record_scope': len(index) - len(nearest),
        'record_to_chosen_neighbor_same_visible_history': exact,
        'record_to_chosen_neighbor_shorter_prefix': shorter_prefix,
        'record_to_chosen_neighbor_longer_cover': longer_cover,
        'record_to_chosen_neighbor_diverges': diverged,
        'first_differing_roles_record_to_neighbor': dict(role_pairs),
        'scope': 'Completed retained decision plan and source objects. Publication membership is checked separately.',
        'interpretation': 'Nearest neighbors are per record within identical scope. Each pair may be referenced twice. '
                          'Distinct opening text is a text count. Task and branch interpretation requires manual reading.',
    }
    dump(args.output / 'report.json', summary)
    (args.output / 'SUCCEEDED').write_text('The selected family was read completely and hash-checked.\n')
    print(json.dumps({k: v for k, v in summary.items() if k not in {'family', 'scope', 'interpretation'}}))


if __name__ == '__main__':
    main()
