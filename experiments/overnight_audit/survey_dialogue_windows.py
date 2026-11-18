"""Find entire captured dialogues repeated after earlier exchanges in the same session."""

import argparse
import os
from collections import Counter, defaultdict
from dataclasses import dataclass
from itertools import groupby
from pathlib import Path

import audit_core as audit
import orjson
from select_stratified_branches import read


@dataclass
class Dialogue:
    sub: int
    scope: str
    headers: tuple
    tokens: tuple
    roles: tuple
    call_offsets: list
    original_ids: tuple


def dialogue(node, sub, scope):
    messages, issues = audit.stable_messages(node)
    if issues:
        return None
    original_ids = tuple(c['id'] for m in node['messages'] if m['role'] == 'assistant'
                         for c in (m.get('tool_calls') or ()))
    sequence, roles, call_offsets = [], [], [0]
    seen = 0
    for msg in messages:
        role = msg['role']
        if role == 'assistant' and msg.get('tool_calls'):
            calls = []
            for call in msg['tool_calls']:
                assert call['id'] == ['ordinal', seen]
                calls.append({**call, 'id': 'bound-call'})
                seen += 1
            msg = {**msg, 'tool_calls': calls}
        elif role == 'tool':
            marker, called = msg['tool_call_id']
            assert marker == 'ordinal' and 0 <= called < seen
            # Distance to the bound call survives trimming complete earlier exchanges.
            msg = {**msg, 'tool_call_id': ['preceding_call_distance', seen - called]}
        sequence.append(audit.digest(audit.dumps(msg)))
        roles.append(role)
        call_offsets.append(seen)
    assert seen == len(original_ids)
    start = next((i for i, role in enumerate(roles) if role not in {'system', 'developer'}), len(roles))
    return Dialogue(sub, scope, tuple(sequence[:start]), tuple(sequence[start:]), tuple(roles[start:]),
                    call_offsets[start:], original_ids)


def find_windows(items):
    positions = defaultdict(list)
    for i, item in enumerate(items):
        for k, token in enumerate(item.tokens):
            positions[token].append((i, k))
    for short in items:
        if 'user' not in short.roles or 'assistant' not in short.roles:
            continue
        n = len(short.tokens)
        anchor = min(range(n), key=lambda i: len(positions[short.tokens[i]]))
        for member, where in positions[short.tokens[anchor]]:
            long = items[member]
            start = where - anchor
            end = start + n
            if start <= 0 or end > len(long.tokens) or long.sub == short.sub:
                continue
            if long.tokens[start:end] != short.tokens:
                continue
            before = long.roles[:start]
            if 'user' not in before or 'assistant' not in before:
                continue
            # Equal tokens prove identical roles/visible fields and relative bindings.
            left, right = long.call_offsets[start], long.call_offsets[end]
            ids = long.original_ids[left:right]
            assert len(ids) == len(short.original_ids)
            yield {'short_sub': short.sub, 'long_sub': long.sub, 'header_messages': len(short.headers),
                   'same_headers': short.headers == long.headers,
                   'skipped_dialogue_messages': start, 'short_dialogue_messages': n,
                   'long_dialogue_messages': len(long.tokens), 'trailing_dialogue_messages': len(long.tokens) - end,
                   'short_user': short.roles.count('user'), 'short_assistant': short.roles.count('assistant'),
                   'short_tool_results': short.roles.count('tool'), 'short_tool_calls': len(ids),
                   'original_call_ids_equal': (ids == short.original_ids) if ids else None,
                   'also_at_start': long.tokens[:n] == short.tokens}


def main():
    p = argparse.ArgumentParser()
    p.add_argument('--survey', type=Path, required=True)
    p.add_argument('--prior-cache', type=Path, required=True)
    p.add_argument('--output', type=Path, required=True)
    args = p.parse_args()
    os.umask(0o077)
    parent = orjson.loads((args.survey / 'report.json').read_bytes())
    assert parent['passed'] and (args.survey / 'SUCCEEDED').is_file()
    args.output.mkdir(exist_ok=False)
    dirs = (args.prior_cache, args.survey / 'sources')
    counts, categories, seen_families = Counter(), Counter(), set()
    with ((args.survey / 'members.jsonl').open('rb') as members,
          (args.output / 'windows.jsonl').open('wb') as out):
        for family, rows in groupby(map(orjson.loads, members), key=lambda r: r['family']):
            assert family not in seen_families
            seen_families.add(family)
            by_scope = defaultdict(list)
            for r in rows:
                node = read(r, dirs)
                assert node['metadata']['session_id'] == family
                d = dialogue(node, r['sub'], r['scope_hash'])
                counts['read'] += 1
                counts['bytes'] += r['ref']['bytes']
                if d is None:
                    counts['ambiguous_binding_excluded'] += 1
                else:
                    by_scope[d.scope].append(d)
                    counts['valid_bindings'] += 1
            short_subs, pairs, stronger, terminals = set(), set(), set(), set()
            for group in by_scope.values():
                if len(group) < 2:
                    continue
                counts['records_with_peer_in_scope'] += len(group)
                header_sizes = Counter(d.headers for d in group)
                counts['records_with_an_identical_header_peer'] += sum(n for n in header_sizes.values() if n > 1)
                counts['records_with_a_different_header_peer'] += len(group) if len(header_sizes) > 1 else 0
                for result in find_windows(group):
                    short = result['short_sub']
                    short_subs.add(short)
                    pairs.add((short, result['long_sub']))
                    if result['trailing_dialogue_messages'] == 0:
                        terminals.add(short)
                    if result['short_tool_results'] and result['original_call_ids_equal']:
                        stronger.add(short)
                    place = 'suffix' if result['trailing_dialogue_messages'] == 0 else 'interior'
                    ids = {True: 'same_ids', False: 'changed_ids', None: 'no_calls'}[result['original_call_ids_equal']]
                    header = 'same_headers' if result['same_headers'] else 'different_headers'
                    categories[header + '/' + place + '/' + ids] += 1
                    counts['alignments_also_at_start'] += result['also_at_start']
                    out.write(orjson.dumps({'family': family, **result}) + b'\n')
            counts['pair_count'] += len(pairs)
            counts['short_records'] += len(short_subs)
            counts['short_with_suffix'] += len(terminals)
            counts['short_with_tool_result_and_same_ids'] += len(stronger)
            counts['sessions_with_window'] += bool(pairs)
    assert len(seen_families) == parent['repeated_filename_families']
    assert (counts['read'], counts['bytes']) == (parent['counts']['members'], parent['counts']['source_bytes'])
    report = {'passed': True, 'local_only': True, 'plan_sha256': parent['plan_sha256'],
              'sessions_read': len(seen_families), 'counts': counts, 'alignments': categories,
              'scope': 'Same session, model, harness and user scope. Leading system/developer messages are '
                       'compared and reported separately. Mid-dialogue instructions must match. '
                       'The entire shorter dialogue matches a consecutive block after at least one earlier '
                       'user and one earlier assistant message. Reasoning excluded under the visible-message '
                       'comparison. Tool result bindings retain their exact relative call positions.',
              'limit': 'A matching block can be a repeated exchange or a branch. The metric alone does not '
                       'establish why the earlier history is absent or whether to merge.'}
    (args.output / 'report.json').write_bytes(orjson.dumps(report, option=orjson.OPT_INDENT_2) + b'\n')
    (args.output / 'SUCCEEDED').write_text('Verified full repeated-family sample.\n')
    print(orjson.dumps(report).decode(), flush=True)


if __name__ == '__main__':
    main()
