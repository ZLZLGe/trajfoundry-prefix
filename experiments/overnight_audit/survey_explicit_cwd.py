"""Measure visible prefix pairs differing only by an explicit literal absolute cwd in Bash arguments."""

import argparse
import hashlib
import os
import re
from collections import Counter, defaultdict
from itertools import groupby
from pathlib import Path

import audit_core as audit
import orjson
from explain_family_replacement_characters import candidate_pairs
from select_stratified_branches import read

CD = re.compile(r'''^cd[ \t]+(?:(/[A-Za-z0-9_./-]*)|"(/[A-Za-z0-9_./ -]*)"|'(/[A-Za-z0-9_./ -]*)')[ \t]*&&[ \t]+''')


def split_command(command):
    if not isinstance(command, str):
        return None
    match = CD.match(command)
    if match is None:
        return None
    path = next(p for p in match.groups() if p is not None)
    suffix = command[match.end():]
    if not suffix or suffix[0].isspace() or CD.match(suffix) or any(x in {'.', '..'} for x in path.split('/')):
        return None
    return match[0], suffix


def projection(message):
    changed, calls = {}, []
    for i, call in enumerate(message.get('tool_calls') or ()):
        f = call['function']
        args = f.get('arguments')
        directory = split_command(args.get('command')) if f['name'] == 'Bash' and isinstance(args, dict) else None
        if directory is not None:
            prefix, suffix = directory
            changed[i] = prefix
            call = {**call, 'function': {**f, 'arguments': {**args, 'command': suffix}}}
        calls.append(call)
    return ({**message, 'tool_calls': calls} if changed else message), changed


def compare_members(rows, directories, write):
    intern, tokens, metadata, histories, bindings = {}, {}, [], defaultdict(list), {}
    observed = set()
    for row in rows:
        node = read(row, directories)
        sub = row['sub']
        messages, issues = audit.stable_messages(node)
        originals = iter(m['tool_calls'] for m in node['messages'] if m['role'] == 'assistant' and m.get('tool_calls'))
        seq, call_ids = [], {}
        for i, m in enumerate(messages):
            if m['role'] == 'assistant' and m.get('tool_calls'):
                call_ids[i] = tuple(c.get('id') for c in next(originals))
            encoded = audit.dumps(m)
            token = intern.get(encoded)
            if token is None:
                token = intern[encoded] = len(metadata)
                candidate, stripped = projection(m)
                broad = tokens.setdefault(audit.dumps(candidate), len(tokens))
                metadata.append((broad, stripped))
            seq.append(token)
            if metadata[token][1]:
                observed.add(sub)
        assert next(originals, None) is None
        histories[row['scope_hash']].append((tuple(metadata[t][0] for t in seq), sub, tuple(seq)))
        bindings[sub] = call_ids, issues
    counts, short_records, equal_records = Counter(), set(), set()
    for seqs in histories.values():
        for a, left, b, right in candidate_pairs(seqs):
            counts['broad_pairs'] += 1
            changes = []
            for i, (x, y) in enumerate(zip(left, right)):
                if x == y:
                    continue
                aw, bw = metadata[x][1], metadata[y][1]
                # Two different explicit prefixes must never collapse to the same suffix.
                if any(aw[k] != bw[k] for k in aw.keys() & bw.keys()):
                    break
                distinct = sorted(aw.keys() ^ bw.keys())
                assert distinct
                for k in distinct:
                    side = 'left' if k in aw else 'right'
                    prefix = (aw if k in aw else bw)[k]
                    aid, bid = bindings[a][0][i][k], bindings[b][0][i][k]
                    changes.append({'message': i, 'call': k, 'explicit_side': side,
                                    'prefix_sha256': hashlib.sha256(prefix.encode()).hexdigest(),
                                    'same_original_call_id': bool(aid) and aid == bid})
            else:
                if not changes:
                    counts['already_equal_visible_prefixes'] += 1
                    continue
                kind = 'strict' if len(left) < len(right) else 'equal_length'
                counts[kind] += 1
                if kind == 'strict':
                    short_records.add(a)
                else:
                    equal_records.update((a, b))
                write({'left': a, 'right': b, 'kind': kind, 'lengths': [len(left), len(right)],
                       'changes': changes, 'binding_issue_in_either_history': bool(bindings[a][1] or bindings[b][1])})
    return {'members': len(bindings), 'members_with_literal_cd': len(observed), 'distinct_shorter': len(short_records),
            'distinct_equal_members': len(equal_records), 'pairs': dict(counts)}


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
    totals, pairs, seen = Counter(), Counter(), set()
    with ((args.survey / 'members.jsonl').open('rb') as members,
          (args.output / 'families.jsonl').open('wb') as family_results,
          (args.output / 'witnesses.jsonl').open('wb') as witnesses):
        for family, rows in groupby(map(orjson.loads, members), key=lambda row: row['family']):
            assert family not in seen
            seen.add(family)

            def write(pair):
                witnesses.write(orjson.dumps({'family': family, **pair}) + b'\n')

            summary = compare_members(rows, (args.prior_cache, args.survey / 'sources'), write)
            family_results.write(orjson.dumps({'family': family, **summary}) + b'\n')
            totals.update({k: v for k, v in summary.items() if k != 'pairs'})
            totals['families_with_strict'] += bool(summary['distinct_shorter'])
            totals['families_with_equal_length'] += bool(summary['distinct_equal_members'])
            pairs.update(summary['pairs'])
            if len(seen) == 1 or len(seen) % 500 == 0:
                print(f'families={len(seen)} records={totals["members"]}', flush=True)
    assert len(seen) == parent['repeated_filename_families'] and totals['members'] == parent['counts']['members']
    report = {'passed': True, 'plan_sha256': parent['plan_sha256'], 'families': len(seen), 'totals': totals,
              'pairs': pairs, 'rule': 'Only a single literal absolute cd prefix in a Bash command is made optional. '
              'Other canonical fields and full visible overlap must agree. Actual working directories are unknown.'}
    (args.output / 'report.json').write_bytes(orjson.dumps(report, option=orjson.OPT_INDENT_2) + b'\n')
    (args.output / 'SUCCEEDED').write_text('All members verified locally against the retained plan.\n')
    print(orjson.dumps(report).decode(), flush=True)


if __name__ == '__main__':
    main()
