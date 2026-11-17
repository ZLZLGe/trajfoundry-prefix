"""Collect complete local evidence for equal-length and selected masking removals.

This is a reading aid, not another merge verifier. It compares every visible
field without applying the masking matcher, and makes no semantic verdict.
"""
import argparse
import collections
import json
import os
import re
import threading
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import audit_core as audit

from trajectory_prefix.storage import Reader, StorageConfig

MARKER = re.compile(r'<[A-Z][A-Z_0-9]*>|\[[A-Z][A-Z_0-9 -]*\]')


def changes(left, right, path=''):
    if left == right:
        return
    if isinstance(left, dict) and isinstance(right, dict):
        for key in sorted(left.keys() | right.keys()):
            if key not in left or key not in right:
                yield {'path': path + '/' + key, 'kind': 'missing_key'}
            else:
                yield from changes(left[key], right[key], path + '/' + key)
        return
    if isinstance(left, list) and isinstance(right, list) and len(left) == len(right):
        for i, (a, b) in enumerate(zip(left, right)):
            yield from changes(a, b, path + '/' + str(i))
        return
    row = {'path': path, 'left_type': type(left).__name__, 'right_type': type(right).__name__}
    if isinstance(left, str) and isinstance(right, str):
        start = 0
        while start < min(len(left), len(right)) and left[start] == right[start]:
            start += 1
        end = 0
        while end < min(len(left), len(right)) - start and left[-end - 1] == right[-end - 1]:
            end += 1
        row.update(lengths=[len(left), len(right)], common_prefix_chars=start, common_suffix_chars=end,
                   changed_spans=[audit.safe_excerpt(s[start:len(s) - end], 1600) for s in [left, right]],
                   whole_field_marker=[bool(MARKER.fullmatch(s.strip())) for s in [left, right]])
    else:
        row.update(values=[audit.safe_excerpt(v, 1600) for v in [left, right]])
    yield row


def select_examples(rows):
    selections = []
    rare = {'<MASK>', '[REDACTED]', '<SENSITIVE>', '<ID_NUMBER>', '<COOKIE>'}
    for channel in sorted({r['record']['dataset'] for r in rows}):
        candidates = [r for r in rows if r['record']['dataset'] == channel]
        chosen = []
        strata = [
            ('equal_length', lambda r: r['message_counts'][0] == r['message_counts'][1],
             lambda r: (-len(r['differences']), r['record']['id'])),
            ('without_session', lambda r: not r['has_session'],
             lambda r: (-len(r['differences']), r['record']['id'])),
            ('unusual_marker', lambda r: bool(set(r['necessary_labels']) & rare),
             lambda r: (-len(set(r['necessary_labels']) & {'<MASK>', '[REDACTED]', '<COOKIE>'}),
                        len(r['differences']), r['record']['id'])),
            ('system_or_tool_content', lambda r: any(d['role'] in {'system', 'tool'} for d in r['differences']),
             lambda r: (-sum(d['role'] == 'system' for d in r['differences']),
                        len(r['differences']), r['record']['id'])),
            ('largest_remaining_difference_count', lambda r: True,
             lambda r: (-len(r['differences']), r['record']['id'])),
        ]
        for name, predicate, key in strata:
            eligible = sorted((r for r in candidates if predicate(r)
                               and r['record']['id'] not in {v['record']['id'] for v in chosen}), key=key)
            if eligible:
                chosen.append({**eligible[0], 'selection_reason': name})
            if len(chosen) == 4:
                break
        selections.extend(chosen)
    return selections


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--publication', type=Path, required=True)
    parser.add_argument('--config', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--only-short-extensions', action='store_true',
                        help='Read every removal with at most six messages and a longer witness.')
    args = parser.parse_args()
    os.umask(0o077)
    args.output.mkdir(parents=True, exist_ok=False)
    rows = []
    for channel in ['free-router', 'boyue-rich', 'token-plan']:
        root = args.publication / channel / 'refined'
        assert (root / 'SUCCEEDED').is_file()
        report = json.loads((root / 'report.json').read_text())
        assert report['passed']
        current = [json.loads(line) for part in sorted((root / 'explanations').glob('part-*'))
                   for line in part.open()]
        assert len(current) == report['mask_additional_removed']
        rows.extend(current)
    equal = [r for r in rows if r['message_counts'][0] == r['message_counts'][1]]
    if args.only_short_extensions:
        selected = []
        wanted = {r['record']['id']: r for r in rows if r['message_counts'][0] <= 6
                  and r['message_counts'][0] < r['message_counts'][1]}
    else:
        selected = select_examples(rows)
        wanted = {r['record']['id']: r for r in equal}
        wanted.update({r['record']['id']: r for r in selected})
    config = json.loads(args.config.read_text())
    storage = StorageConfig(endpoint_url=config['s3_endpoint'], region=config['s3_region'],
                            credentials_file=config['s3_credentials_file'], cache_dir=str(args.output / 'sources'))
    local = threading.local()

    def read(row):
        if not hasattr(local, 'reader'):
            local.reader = Reader(storage)
        nodes = [local.reader.node(row[k]) for k in ['record', 'retained']]
        stable = [audit.stable_messages(n)[0] for n in nodes]
        same_scope = audit.scope(nodes[0], row['record']) == audit.scope(nodes[1], row['retained'])
        assert same_scope
        differences = []
        for i, (a, b) in enumerate(zip(*stable)):
            differences.extend({'message_index': i, 'role': a['role'], **d} for d in changes(a, b))
        whole_assistant = [i for i, (a, b) in enumerate(zip(*stable)) if a['role'] == b['role'] == 'assistant'
                           and a['content'] != b['content']
                           and any(MARKER.fullmatch(m['content'].strip()) for m in [a, b])]
        visible_answers = [[m for m in seq if m['role'] == 'assistant'] for seq in stable]
        packet = {**row, 'original_message_counts': [len(n['messages']) for n in nodes],
                  'independent_message_counts': list(map(len, stable)),
                  'same_identity_scope': same_scope,
                  'first_user_excerpts': [next((audit.safe_excerpt(m['content'], 2400) for m in seq
                                               if m['role'] == 'user'), '') for seq in stable],
                  'whole_assistant_marker_positions': whole_assistant,
                  'all_visible_assistants_equal': visible_answers[0] == visible_answers[1],
                  'independent_field_differences': differences,
                  'automatic_semantic_verdict': None, 'source_hashes_verified': True}
        return packet

    with ThreadPoolExecutor(max_workers=6) as pool:
        packets = list(pool.map(read, [wanted[k] for k in sorted(wanted)]))
    equal_ids = {r['record']['id'] for r in equal}
    equal_packets = [p for p in packets if p['record']['id'] in equal_ids]
    by_channel = collections.Counter(r['record']['dataset'] for r in equal_packets)
    whole = collections.Counter(r['record']['dataset'] for r in equal_packets if r['whole_assistant_marker_positions'])
    report = {'passed': True, 'local_only': True, 's3_writes': False,
              'selection_scope': 'all_short_extensions' if args.only_short_extensions else 'all_equal_and_selected_extensions',
              'all_mask_removals': len(rows), 'all_equal_length_removals': len(equal),
              'read_equal_length_pairs': len(equal_packets),
              'equal_length_by_channel': dict(by_channel), 'selected_manual_pairs': len(selected),
              'complete_pairs_read': len(packets), 'same_visible_assistants_equal_length_pairs':
              sum(p['all_visible_assistants_equal'] for p in equal_packets),
              'whole_assistant_marker_equal_length_by_channel': dict(whole),
              'limitation': 'Hidden values cannot be reconstructed from placeholders. No automatic correctness verdict.'}
    for name, value in [('report.json', report), ('reading-packets.json', packets), ('selected.json', selected)]:
        (args.output / name).write_text(json.dumps(value, ensure_ascii=False, indent=2) + '\n')
    (args.output / 'SUCCEEDED').write_text('All selected complete objects read and SHA256 verified locally.\n')
    print(json.dumps(report, ensure_ascii=False))


if __name__ == '__main__':
    main()
