"""Describe changed user-message slots using already verified local objects.

No network access or merge decision. Slot names locate the difference only;
matching a mask to a visible value does not recover the hidden original value.
"""

import argparse
import collections
import hashlib
import json
import os
import re
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import audit_core as audit

SLOTS = [
    ('timezone', re.compile(r'Asia/(?P<value><LOCATION>|[A-Za-z_]+)')),
    ('sender_id', re.compile(r'User ID: (?P<value><ID_NUMBER>|<PHONE>|[0-9]+), Nickname:')),
    ('bot_identity', re.compile(r'【你的身份】[^\n]*?」(?P<value>（[0-9]+|<PHONE>)）。')),
]


def tokenize(text, active):
    matches = sorted((match.start('value'), match.end('value'), name, match.group('value'))
                     for name, pattern in SLOTS if name in active for match in pattern.finditer(text))
    result, end = [], 0
    for start, stop, name, value in matches:
        assert start >= end
        result.extend([text[end:start], (name, value)])
        end = stop
    result.append(text[end:])
    return result


def slot_difference(left, right):
    aa, bb = [tokenize(text, {name for name, _ in SLOTS}) for text in (left, right)]
    if len(aa) != len(bb):
        return None
    changed = set()
    for a, b in zip(aa, bb):
        if a == b:
            continue
        if not isinstance(a, tuple) or not isinstance(b, tuple) or a[0] != b[0]:
            return None
        if not any(value in {'<LOCATION>', '<ID_NUMBER>', '<PHONE>'} for value in (a[1], b[1])):
            return None
        changed.add(a[0])
    return sorted(changed) if changed else None


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--input', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    assert (args.input / 'SUCCEEDED').exists()
    original = json.loads((args.input / 'report.json').read_text())
    assert original['passed']
    os.umask(0o077)
    args.output.mkdir(parents=True, exist_ok=False)
    rows = json.loads((args.input / 'other-user-text-pairs.json').read_text())
    assert len(rows) == original['other_user_text_pairs']

    def read(row):
        nodes = []
        for side in ('record', 'retained'):
            ref = row[side]
            data = (args.input / 'sources' / ref['sha256']).read_bytes()
            assert len(data) == ref['bytes'] and hashlib.sha256(data).hexdigest() == ref['sha256']
            nodes.append(json.loads(data))
        messages = [audit.stable_messages(node)[0] for node in nodes]
        details = []
        for change in row['user_changes']:
            index = change['message_index']
            texts = [seq[index]['content'] for seq in messages]
            slots = slot_difference(*texts)
            details.append({'message_index': index,
                            'category': '+'.join(slots) if slots else 'other_text',
                            'slots': slots})
        categories = sorted({d['category'] for d in details})
        calls = [[call.get('id') for message in node['messages']
                  for call in message.get('tool_calls', [])] for node in nodes]
        ids_same = bool(calls[0]) and len(calls[0]) <= len(calls[1]) and calls[0] == calls[1][:len(calls[0])]
        return {**row, 'user_field_locations': details,
                'location_summary': ('includes_other_text' if 'other_text' in categories
                                     else '+'.join(sorted({slot for d in details for slot in d['slots']}))),
                'original_tool_call_counts': list(map(len, calls)),
                'original_tool_call_id_prefix_equal': ids_same}

    counts, field_counts, packets = collections.Counter(), collections.Counter(), []
    for key, count in original['pair_counts'].items():
        if key.endswith('/exact_shanghai_timezone_spelling_only'):
            counts[key.split('/')[0] + '/timezone'] += count
    with ThreadPoolExecutor(max_workers=6) as pool, (args.output / 'pairs.jsonl').open('w') as out:
        for result in pool.map(read, rows):
            packets.append(result)
            dataset = result['record']['dataset']
            counts[dataset + '/' + result['location_summary']] += 1
            for difference in result['user_field_locations']:
                field_counts[dataset + '/' + difference['category']] += 1
            out.write(json.dumps(result, ensure_ascii=False) + '\n')
    remaining = [p for p in packets if p['location_summary'] == 'includes_other_text']
    report = {'passed': True, 'local_only': True, 'network_access': False,
              'all_user_changed_pairs': original['pairs_with_changed_user_text'],
              'previously_explained_exact_timezone_pairs': sum(
                  n for k, n in original['pair_counts'].items() if k.endswith('/exact_shanghai_timezone_spelling_only')),
              'complete_cached_pairs_reread': len(packets), 'all_pair_location_counts': dict(counts),
              'subdivision_message_location_counts': dict(field_counts),
              'remaining_other_text_pairs': len(remaining),
              'remaining_original_tool_id_prefix_equal': sum(p['original_tool_call_id_prefix_equal'] for p in remaining),
              'limitation': 'Locations explain user-text differences only, without a semantic correctness verdict.'}
    assert sum(counts.values()) == original['pairs_with_changed_user_text']
    (args.output / 'other-text-pairs.json').write_text(json.dumps(remaining, ensure_ascii=False, indent=2) + '\n')
    (args.output / 'report.json').write_text(json.dumps(report, ensure_ascii=False, indent=2) + '\n')
    (args.output / 'SUCCEEDED').write_text('All additional cached sources verified and user changes located.\n')
    print(json.dumps(report, ensure_ascii=False))


if __name__ == '__main__':
    main()
