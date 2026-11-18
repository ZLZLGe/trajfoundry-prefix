"""Compare a retained short-side call's explicit directory to earlier textual working-directory declarations."""

import argparse
import hashlib
import os
import re
from collections import Counter, defaultdict
from pathlib import Path

import orjson
from select_stratified_branches import read
from survey_explicit_cwd import CD

SYSTEM = re.compile(r'^\s*(?:-\s+)?Primary working directory: (/[^\r\n]*?)(?: \(was /[^\r\n]*\))?\s*$')
RESET = re.compile(r'^\s*Shell cwd was reset to (/[^\r\n]*?)\s*$')


def declarations(messages):
    primary = reset = latest = None
    for msg in messages:
        text = msg['content']
        if msg['role'] in {'system', 'developer'} and 'Primary working directory: ' in text:
            for line in text.splitlines():
                if m := SYSTEM.fullmatch(line):
                    primary = latest = m[1]
        elif msg['role'] == 'tool' and 'Shell cwd was reset to ' in text:
            for line in text.splitlines():
                if m := RESET.fullmatch(line):
                    reset = latest = m[1]
    return primary, reset, latest


def relation(paths, prior):
    if prior is None:
        return 'no_line'
    if paths == {prior}:
        return 'all_equal'
    if prior in paths:
        return 'some_equal'
    if all(path.startswith(prior.rstrip('/') + '/') for path in paths):
        return 'all_descendants'
    return 'other'


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--survey', type=Path, required=True)
    parser.add_argument('--cwd', type=Path, required=True)
    parser.add_argument('--prior-cache', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    os.umask(0o077)
    assert (args.cwd / 'SUCCEEDED').is_file()
    parent = orjson.loads((args.cwd / 'report.json').read_bytes())
    changes = defaultdict(set)
    with (args.cwd / 'witnesses.jsonl').open('rb') as stream:
        for w in map(orjson.loads, stream):
            assert w['kind'] == 'strict'
            for ch in w['changes']:
                assert ch['explicit_side'] == 'left' and ch['message'] == w['lengths'][0] - 1
                changes[w['family'], w['left']].add((ch['call'], ch['prefix_sha256']))
    assert len(changes) == parent['totals']['distinct_shorter']
    counts = {k: Counter() for k in ('system_primary', 'tool_reset', 'latest_either')}
    models = Counter()
    args.output.mkdir(parents=True, exist_ok=False)
    with ((args.survey / 'members.jsonl').open('rb') as members, (args.output / 'shorts.jsonl').open('wb') as out):
        for row in map(orjson.loads, members):
            key = row['family'], row['sub']
            if key not in changes:
                continue
            node = read(row, (args.prior_cache, args.survey / 'sources'))
            models[node['model']] += 1
            messages = node['messages']
            assert messages[-1]['role'] == 'assistant'
            paths = set()
            for call_index, expected_prefix in changes.pop(key):
                call = messages[-1]['tool_calls'][call_index]['function']
                assert call['name'] == 'Bash'
                command = call['arguments']['command']
                match = CD.match(command)
                assert match and hashlib.sha256(match[0].encode()).hexdigest() == expected_prefix
                paths.add(next(p for p in match.groups() if p is not None))
            labels = {k: relation(paths, path) for k, path in zip(counts, declarations(messages[:-1]))}
            for k, label in labels.items():
                counts[k][label] += 1
            out.write(orjson.dumps({'family': key[0], 'sub': key[1], 'directories': len(paths), **labels}) + b'\n')
    assert not changes and models.total() == parent['totals']['distinct_shorter']
    report = {'passed': True, 'records': models.total(), 'plan_sha256': parent['plan_sha256'],
              'relative_to_earlier_text': counts, 'models': models,
              'limit': 'Literal path comparisons against prior system/developer primary-working-directory '
                       'or tool reset lines. Declarations do not establish the actual process cwd.'}
    (args.output / 'report.json').write_bytes(orjson.dumps(report, option=orjson.OPT_INDENT_2) + b'\n')
    (args.output / 'SUCCEEDED').write_text('All short witnesses checked against preceding messages.\n')
    print(orjson.dumps(report).decode())


if __name__ == '__main__':
    main()
