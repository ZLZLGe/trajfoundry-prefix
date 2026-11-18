"""Read one published date locally and index visible histories with documented Edit defaults."""

import argparse
import hashlib
import os
from collections import Counter
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path

import audit_core as audit
import orjson

from trajectory_prefix.projection import Policy, project
from trajectory_prefix.storage import Reader, StorageConfig, reference


def optional_false(node):
    definitions = [tool for tool in node.get('tools') or [] if tool.get('name') == 'Edit']
    if len(definitions) != 1:
        return False
    schema = definitions[0].get('parameters') or {}
    prop = (schema.get('properties') or {}).get('replace_all') or {}
    return (isinstance(prop, dict) and prop.get('type') == 'boolean' and prop.get('default') is False
            and isinstance(schema.get('required', []), list) and 'replace_all' not in schema.get('required', []))


def calls(msg):
    for i, call in enumerate(msg.get('tool_calls') or []):
        f = call.get('function') or {}
        if f.get('name') == 'Edit' and isinstance(f.get('arguments'), dict):
            yield i, f['arguments']


def states(node):
    counts = Counter()
    for msg in node['messages']:
        if msg['role'] != 'assistant':
            continue
        for _, args in calls(msg):
            if 'replace_all' not in args:
                counts['omitted'] += 1
            elif args['replace_all'] is False:
                counts['false'] += 1
            elif args['replace_all'] is True:
                counts['true'] += 1
            else:
                counts['other'] += 1
    return counts


def folded_history(messages):
    original, folded = [], []
    for msg in messages:
        token = audit.digest(audit.dumps(msg))
        original.append(token)
        changed = {i for i, args in calls(msg) if args.get('replace_all') is False}
        if not changed:
            folded.append(token)
            continue
        batch = list(msg['tool_calls'])
        for i in changed:
            call = batch[i]
            f = call['function']
            args = {k: v for k, v in f['arguments'].items() if k != 'replace_all'}
            batch[i] = {**call, 'function': {**f, 'arguments': args}}
        folded.append(audit.digest(audit.dumps({**msg, 'tool_calls': batch})))
    return b''.join(original), b''.join(folded)


def init_worker(storage):
    global READER
    READER = Reader(storage)


def index_chunk(job):
    name, refs = job
    counts = Counter()
    with name.with_suffix('.tmp').open('wb') as out:
        for ref in refs:
            node = READER.node(ref)
            counts['read'] += 1
            counts['bytes'] += ref['bytes']
            observed = states(node)
            if not observed:
                continue
            counts['records_with_edit'] += 1
            counts.update({'calls_' + k: v for k, v in observed.items()})
            counts.update({'records_' + k: 1 for k in observed})
            if not optional_false(node):
                continue
            counts['records_with_edit_and_default'] += 1
            counts.update({'declared_calls_' + k: v for k, v in observed.items()})
            counts.update({'declared_records_' + k: 1 for k in observed})
            if 'false' not in observed and 'omitted' not in observed:
                continue
            messages, issues = audit.stable_messages(node)
            if issues:
                counts['relevant_with_binding_issues'] += 1
                continue
            original, folded = folded_history(messages)
            guard = project(node, ref['dataset'], ref['date'], Policy(extra_information='messages_only'))
            row = {'path': ref['path'], 'sha256': ref['sha256'], 'scope': guard.scope.hex(),
                   'original': original.hex(), 'folded': folded.hex(), 'isolation': guard.isolation_reason}
            out.write(orjson.dumps(row) + b'\n')
            counts['indexed'] += 1
    name.with_suffix('.tmp').replace(name)
    return counts


def main():
    p = argparse.ArgumentParser()
    p.add_argument('--publication', required=True, type=Path)
    p.add_argument('--membership', required=True, type=Path)
    p.add_argument('--config', required=True, type=Path)
    p.add_argument('--output', required=True, type=Path)
    p.add_argument('--workers', type=int, default=10)
    args = p.parse_args()
    os.umask(0o077)
    member = orjson.loads(args.membership.read_bytes())
    assert member['passed'] and member['all_published_member_references_match']
    cfg = orjson.loads(args.config.read_bytes())
    storage = StorageConfig(cfg['s3_endpoint'], cfg['s3_region'], cfg['s3_credentials_file'])
    local_manifest = args.publication / 'published/normalized-manifest.json'
    uri = member['output']
    manifest = orjson.loads(Reader(storage).read({'uri': uri + 'manifest.json', 'bytes': local_manifest.stat().st_size,
                                                'sha256': member['target_manifest_sha256']}))
    assert manifest['status'] == 'SUCCEEDED'
    date = uri.rstrip('/').split('/')[-1].removeprefix('dt=')
    refs = [reference({**f, 'dataset': member['channel'], 'date': date, 'uri': uri + f['path']})
            for f in manifest['files'] if f['path'] != 'lineage.jsonl']
    assert len(refs) == len({r['path'] for r in refs}) == member['counts']['C'] == manifest['record_count']
    assert sum(r['bytes'] for r in refs) == member['trajectory_bytes']
    args.output.mkdir(exist_ok=False)
    parts = args.output / 'index'
    parts.mkdir()
    size = 256
    jobs = [(parts / f'part-{i:07d}.jsonl', refs[i:i + size]) for i in range(0, len(refs), size)]
    total = Counter()
    with ProcessPoolExecutor(max_workers=args.workers, initializer=init_worker, initargs=(storage,)) as pool:
        for i, counts in enumerate(pool.map(index_chunk, jobs), 1):
            total.update(counts)
            if i % 80 == 0:
                print(f'verified={total["read"]}/{len(refs)} indexed={total["indexed"]}', flush=True)
    assert total['read'] == len(refs) and total['bytes'] == member['trajectory_bytes']
    result = {'passed': True, 'local_only': True, 'uri': uri, 'counts': total,
              'manifest_sha256': member['target_manifest_sha256'], 'parts': len(jobs),
              'audit_plan_sha256': hashlib.sha256(b''.join(bytes.fromhex(r['sha256']) for r in refs)).hexdigest()}
    (args.output / 'report.json').write_bytes(orjson.dumps(result, option=orjson.OPT_INDENT_2) + b'\n')
    (args.output / 'SUCCEEDED').write_text('All published trajectories read and verified.\n')
    print(orjson.dumps(result).decode())


if __name__ == '__main__':
    main()
