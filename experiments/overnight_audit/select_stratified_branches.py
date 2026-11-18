"""Freeze a reproducible set of branch pairs for independent human reading from cached sources."""

import argparse
import hashlib
import os
from collections import Counter, defaultdict
from itertools import pairwise
from pathlib import Path

import audit_core as audit
import orjson

SEED = b'deepinfra-20261030-stratified-branch-review-v1\0'


def band(size):
    if size == 2:
        return '2'
    if size < 10:
        return '3-9'
    if size < 100:
        return '10-99'
    return '100+'


def read(obj, directories):
    ref = obj['ref']
    path = next(d / ref['sha256'] for d in directories if (d / ref['sha256']).is_file())
    data = path.read_bytes()
    assert len(data) == ref['bytes'] and hashlib.sha256(data).hexdigest() == ref['sha256']
    node = orjson.loads(data)
    assert obj['scope_hash'] == hashlib.sha256(audit.dumps(audit.scope(node, ref))).hexdigest()
    return node


def closest(members, directories):
    groups = defaultdict(list)
    info = {}
    for member in members:
        node = read(member, directories)
        stable, issues = audit.stable_messages(node)
        sub = member['sub']
        groups[member['scope_hash']].append((tuple(audit.digest(audit.dumps(m)) for m in stable), sub))
        info[sub] = {'ref': member['ref'], 'sub': sub, 'model': node['model'], 'harness': node['harness'],
                     'raw_messages': len(node['messages']), 'visible_messages': len(stable), 'binding_issues': issues}
    best = None
    for seqs in groups.values():
        for (a, x), (b, y) in pairwise(sorted(seqs)):
            common = next((i for i, (s, t) in enumerate(zip(a, b)) if s != t), min(len(a), len(b)))
            lo, hi = sorted((x, y))
            choice = (-common, lo, hi)
            if best is None or choice < best:
                best = choice
    assert best is not None
    neg_common, lo, hi = best
    return {'visible_prefix': -neg_common, 'left': info[lo], 'right': info[hi]}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--survey', type=Path, required=True)
    parser.add_argument('--prior-cache', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    os.umask(0o077)
    assert (args.survey / 'SUCCEEDED').is_file()
    parent = orjson.loads((args.survey / 'report.json').read_bytes())
    families = defaultdict(list)
    with (args.survey / 'members.jsonl').open('rb') as stream:
        for row in map(orjson.loads, stream):
            families[row['family']].append(row)
    assert sum(map(len, families.values())) == parent['counts']['members']
    strata = defaultdict(list)
    for family, members in families.items():
        if max(Counter(m['scope_hash'] for m in members).values()) >= 2:
            strata[band(len(members))].append(family)
    selected = []
    directories = (args.prior_cache, args.survey / 'sources')
    for size in ('2', '3-9', '10-99', '100+'):
        ordered = sorted(strata[size], key=lambda f: (hashlib.sha256(SEED + f.encode()).digest(), f))
        assert len(ordered) >= 4
        for family in ordered[:4]:
            selected.append({'case': len(selected) + 1, 'band': size, 'family': family,
                             'family_size': len(families[family]), **closest(families[family], directories)})
    args.output.mkdir(parents=True, exist_ok=False)
    report = {'plan_sha256': parent['plan_sha256'], 'size_bands': {k: len(v) for k, v in strata.items()},
              'total_families': len(families), 'pairs': selected,
              'selection': 'Four families per size band using a fixed SHA256 sample, restricted to at least '
                           'one same-scope pair. Within each family choose a pair with the longest shared visible '
                           'prefix, using adjacent sorted histories and stable sub-number tie breakers.'}
    (args.output / 'selected.json').write_bytes(orjson.dumps(report, option=orjson.OPT_INDENT_2) + b'\n')
    print(orjson.dumps({'pairs': len(selected), 'eligible_by_band': report['size_bands']}).decode())


if __name__ == '__main__':
    main()
