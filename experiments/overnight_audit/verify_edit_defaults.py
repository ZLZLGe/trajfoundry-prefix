"""Check every additional whole-prefix pair against its published Boyue JSON objects."""

import argparse
import hashlib
import os
from collections import Counter, defaultdict
from functools import lru_cache
from pathlib import Path

import audit_core as audit
import orjson
from index_edit_defaults import optional_false

from trajectory_prefix.storage import Reader, StorageConfig, reference


def matches(group):
    ancestors = []
    for row in sorted(group, key=lambda r: (r['folded'], r['path'])):
        seq = row['folded']
        while ancestors and not seq.startswith(ancestors[-1]['folded']):
            ancestors.pop()
        for prior in ancestors:
            if not row['original'].startswith(prior['original']):
                yield prior, row
        ancestors.append(row)


def comparable(node):
    assert optional_false(node)
    messages, issues = audit.stable_messages(node)
    assert not issues
    original = iter(m['tool_calls'] for m in node['messages'] if m['role'] == 'assistant' and m.get('tool_calls'))
    ids = {i: [c['id'] for c in next(original)] for i, msg in enumerate(messages)
           if msg['role'] == 'assistant' and msg.get('tool_calls')}
    assert next(original, None) is None
    return messages, ids


def prove(left, right):
    lm, li = left
    rm, ri = right
    assert len(lm) <= len(rm)
    changes = []
    for pos, (a, b) in enumerate(zip(lm, rm)):
        if audit.dumps(a) == audit.dumps(b):
            continue
        assert a['role'] == b['role'] == 'assistant' and len(a['tool_calls']) == len(b['tool_calls'])
        assert audit.dumps({k: v for k, v in a.items() if k != 'tool_calls'}) == audit.dumps(
            {k: v for k, v in b.items() if k != 'tool_calls'})
        for idx, (x, y) in enumerate(zip(a['tool_calls'], b['tool_calls'])):
            if audit.dumps(x) == audit.dumps(y):
                continue
            xf, yf = x['function'], y['function']
            assert xf['name'] == yf['name'] == 'Edit'
            xa, ya = xf['arguments'], yf['arguments']
            if 'replace_all' in xa:
                assert xa['replace_all'] is False and 'replace_all' not in ya
                explicit, omitted, side = x, y, 'short'
            else:
                assert ya['replace_all'] is False
                explicit, omitted, side = y, x, 'long'
            f = explicit['function']
            args = dict(f['arguments'])
            del args['replace_all']
            assert audit.dumps({**explicit, 'function': {**f, 'arguments': args}}) == audit.dumps(omitted)
            changes.append({'position': pos, 'call': idx, 'false_on': side,
                            'same_original_id': bool(li[pos][idx]) and li[pos][idx] == ri[pos][idx]})
    assert changes
    return changes, [len(lm), len(rm)]


def main():
    p = argparse.ArgumentParser()
    p.add_argument('--index', required=True, type=Path)
    p.add_argument('--publication', required=True, type=Path)
    p.add_argument('--config', required=True, type=Path)
    p.add_argument('--output', required=True, type=Path)
    args = p.parse_args()
    os.umask(0o077)
    report = orjson.loads((args.index / 'report.json').read_bytes())
    assert report['passed'] and (args.index / 'SUCCEEDED').is_file()
    data = (args.publication / 'published/normalized-manifest.json').read_bytes()
    assert hashlib.sha256(data).hexdigest() == report['manifest_sha256']
    manifest = orjson.loads(data)
    uri, day = report['uri'], report['uri'].rstrip('/').split('dt=')[-1]
    available = {r['path']: r for r in manifest['files']}
    groups, unique = defaultdict(list), set()
    parts = sorted((args.index / 'index').glob('part-*.jsonl'))
    assert len(parts) == report['parts']
    for part in parts:
        with part.open('rb') as stream:
            for row in map(orjson.loads, stream):
                path = row['path']
                assert available[path]['sha256'] == row['sha256'] and path not in unique
                unique.add(path)
                for k in ('original', 'folded'):
                    row[k] = bytes.fromhex(row[k])
                assert len(row['original']) == len(row['folded']) and len(row['folded']) % 32 == 0
                groups[row['scope']].append(row)
    assert len(unique) == report['counts']['indexed']
    args.output.mkdir(exist_ok=False)
    cfg = orjson.loads(args.config.read_bytes())
    reader = Reader(StorageConfig(cfg['s3_endpoint'], cfg['s3_region'], cfg['s3_credentials_file'],
                                  str(args.output / 'sources')))

    @lru_cache(maxsize=32)
    def source(path):
        ref = reference({**available[path], 'dataset': args.publication.name, 'date': day, 'uri': uri + path})
        node = reader.node(ref)
        return comparable(node), audit.dumps(audit.scope(node, ref)), node['model']

    counts, models = Counter(), Counter()
    pairs, short, equal, readers = set(), set(), set(), set()
    removables = defaultdict(set)
    with (args.output / 'witnesses.jsonl').open('wb') as out:
        for group in groups.values():
            for left, right in matches(group):
                a, b = left['path'], right['path']
                assert (a, b) not in pairs
                pairs.add((a, b))
                lm, lid, model = source(a)
                rm, rid, _ = source(b)
                assert lid == rid
                changes, lengths = prove(lm, rm)
                kind = 'strict' if lengths[0] < lengths[1] else 'equal_length'
                isolation = sorted({i for i in [left['isolation'], right['isolation']] if i})
                readers.update([a, b])
                counts[kind] += 1
                counts['pairs_with_isolation'] += bool(isolation)
                counts['all_changed_original_call_ids_same'] += all(c['same_original_id'] for c in changes)
                models[model] += 1
                if kind == 'strict':
                    short.add(a)
                    if not isolation:
                        removables[a].add(b)
                else:
                    equal.update((a, b))
                out.write(orjson.dumps({'short': a, 'long': b, 'kind': kind, 'lengths': lengths, 'model': model,
                                        'isolation': isolation, 'changes': changes}) + b'\n')
    result = {'passed': True, 'uri': uri, 'manifest_sha256': report['manifest_sha256'],
              'input_records': report['counts']['read'], 'indexed': len(unique), 'pair_count': len(pairs),
              'counts': counts, 'distinct_strict_shorter': len(short), 'distinct_equal_members': len(equal),
              'nonisolated_strict_shorter': len(removables), 'models_by_pair': models,
              'verified_source_files': len(readers), 'verified_source_bytes': sum(available[k]['bytes'] for k in readers),
              'nonisolated_shorter_with_external_long': sum(bool(bs - removables.keys()) for bs in removables.values()),
              'limit': 'Requires an explicitly declared optional boolean default of false on both sides, '
                       'unambiguous tool bindings and identical complete visible overlap within scope.'}
    (args.output / 'report.json').write_bytes(orjson.dumps(result, option=orjson.OPT_INDENT_2) + b'\n')
    (args.output / 'SUCCEEDED').write_text('All additional pairs checked against both original normalized files.\n')
    print(orjson.dumps(result).decode())


if __name__ == '__main__':
    main()
