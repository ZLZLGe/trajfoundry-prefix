"""Find shorter histories ending in the same raw tool calls with assistant text present on just one side."""

import argparse
import hashlib
import os
from collections import Counter, defaultdict
from itertools import groupby
from pathlib import Path

import audit_core as audit
import orjson


def inspect_family(members, write):
    tokens, paths, finals, continuing = {}, {}, defaultdict(list), defaultdict(list)
    for sub, scope, node in members:
        stable, binding_issues = audit.stable_messages(node)
        original = iter(m for m in node['messages'] if m['role'] == 'assistant' and m.get('tool_calls'))
        path = 0
        for pos, msg in enumerate(stable):
            if msg['role'] == 'assistant' and msg.get('tool_calls'):
                raw_calls = next(original)['tool_calls']
                key = (scope, path, audit.dumps({k: v for k, v in msg.items() if k != 'content'}),
                       audit.dumps(raw_calls))
                target = finals if pos == len(stable) - 1 else continuing
                target[key].append((sub, len(stable), len(msg['content']), bool(binding_issues)))
            token = tokens.setdefault(audit.dumps(msg), len(tokens))
            # Each edge identifies an exact visible prefix, independent of filename order.
            path = paths.setdefault((path, token), len(paths) + 1)
        assert next(original, None) is None
    counts = Counter()
    shorts = defaultdict(set)
    for key, ends in finals.items():
        for short, short_length, short_chars, short_issue in ends:
            for long, long_length, long_chars, long_issue in continuing.get(key, ()):
                if bool(short_chars) == bool(long_chars):
                    continue
                assert long != short and long_length > short_length
                side = 'short_with_text' if short_chars else 'long_with_text'
                counts[side] += 1
                shorts[side].add(short)
                write({'short': short, 'long': long, 'short_length': short_length, 'long_length': long_length,
                       'text_side': side, 'characters': short_chars or long_chars,
                       'binding_issue_in_either_history': short_issue or long_issue})
    return {'pairs': dict(counts), 'distinct_shorts': {k: len(v) for k, v in shorts.items()},
            'union_distinct_shorts': len(set().union(*shorts.values()))}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--survey', type=Path, required=True)
    parser.add_argument('--prior-cache', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    os.umask(0o077)
    assert (args.survey / 'SUCCEEDED').is_file()
    parent = orjson.loads((args.survey / 'report.json').read_bytes())
    args.output.mkdir(parents=True, exist_ok=False)
    coverage, pairs, shorts = Counter(), Counter(), Counter()
    seen = set()
    with ((args.survey / 'members.jsonl').open('rb') as refs,
          (args.output / 'families.jsonl').open('wb') as families,
          (args.output / 'witnesses.jsonl').open('wb') as witnesses):
        for family, rows in groupby(map(orjson.loads, refs), key=lambda row: row['family']):
            assert family not in seen
            seen.add(family)

            def checked_members():
                for row in rows:
                    ref = row['ref']
                    path = args.prior_cache / ref['sha256']
                    if not path.is_file():
                        path = args.survey / 'sources' / ref['sha256']
                    data = path.read_bytes()
                    assert len(data) == ref['bytes'] and hashlib.sha256(data).hexdigest() == ref['sha256']
                    node = orjson.loads(data)
                    scope = hashlib.sha256(audit.dumps(audit.scope(node, ref))).hexdigest()
                    assert scope == row['scope_hash']
                    coverage['members'] += 1
                    coverage['source_bytes'] += len(data)
                    yield row['sub'], scope, node

            def write(record):
                witnesses.write(orjson.dumps({'family': family, **record}) + b'\n')

            result = inspect_family(checked_members(), write)
            pairs.update(result['pairs'])
            shorts.update(result['distinct_shorts'])
            coverage['distinct_shorts'] += result['union_distinct_shorts']
            coverage['families_with_pairs'] += bool(result['pairs'])
            families.write(orjson.dumps({'family': family, **result}) + b'\n')
            if len(seen) == 1 or len(seen) % 500 == 0:
                print(f'families={len(seen)} sources={coverage["members"]}', flush=True)
    assert len(seen) == parent['repeated_filename_families']
    for key in ('members', 'source_bytes'):
        assert coverage[key] == parent['counts'][key]
    report = {'passed': True, 'plan_sha256': parent['plan_sha256'], 'families': len(seen),
              'coverage': coverage, 'pairs_by_side_with_text': pairs, 'distinct_shorts_by_side_with_text': shorts,
              'rule': 'Strict extension. Every earlier visible message agrees. At the last short-side assistant '
                      'message only visible text may differ, and exactly one side is empty. Raw tool-call arrays '
                      'including IDs and all canonical non-content fields at that position must agree.'}
    (args.output / 'report.json').write_bytes(orjson.dumps(report, option=orjson.OPT_INDENT_2) + b'\n')
    (args.output / 'SUCCEEDED').write_text('All repeated-family members checked from local source caches.\n')
    print(orjson.dumps(report).decode(), flush=True)


if __name__ == '__main__':
    main()
