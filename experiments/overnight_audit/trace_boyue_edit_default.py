"""Trace an Edit default difference to Tool Use records in the original Boyue archives."""

import argparse
import hashlib
import os
from collections import Counter, defaultdict
from contextlib import closing
from pathlib import Path

import audit_core as audit
import orjson
from trajfoundry.canonical import trajectory_id
from trajfoundry.output_contract import parse_trajectory_record
from trajfoundry.spark.source import _input_client, parse_s3_uri
from verify_edit_defaults import comparable, prove


def obj(value):
    return orjson.loads(value) if isinstance(value, str) else value


def edit_args(message, identifier):
    blocks = message.get('content') or []
    if isinstance(blocks, list):
        for b in blocks:
            if isinstance(b, dict) and b.get('type') == 'tool_use' and b.get('name') == 'Edit' and b.get('id') == identifier:
                yield b['input']
    for c in message.get('tool_calls') or []:
        f = c.get('function', {})
        if c.get('id') == identifier and f.get('name') == 'Edit':
            yield obj(f['arguments'])


def observed_calls(raw, identifier):
    request, response = (obj(raw[k]) for k in ('request_body', 'response_body'))
    for m in request.get('messages', []):
        if m.get('role') == 'assistant':
            yield from (('request_history', a) for a in edit_args(m, identifier))
    for item in request.get('input', []) if isinstance(request.get('input'), list) else ():
        if item.get('type') == 'function_call' and item.get('name') == 'Edit' and item.get('call_id') == identifier:
            yield 'request_history', obj(item['arguments'])
    if response.get('role') == 'assistant':
        yield from (('response', a) for a in edit_args(response, identifier))
    for choice in response.get('choices', []):
        yield from (('response', a) for a in edit_args(choice.get('message', {}), identifier))
    body = response.get('response') if response.get('type', '').startswith('response.') else response
    for item in (body or {}).get('output', []):
        if item.get('type') == 'function_call' and item.get('name') == 'Edit' and item.get('call_id') == identifier:
            yield 'response', obj(item['arguments'])


def main():
    p = argparse.ArgumentParser()
    p.add_argument('--examples', required=True, type=Path)
    p.add_argument('--case', required=True)
    p.add_argument('--membership', required=True, type=Path)
    p.add_argument('--publication', required=True, type=Path)
    p.add_argument('--raw-root', required=True)
    p.add_argument('--config', required=True, type=Path)
    p.add_argument('--output', required=True, type=Path)
    args = p.parse_args()
    os.umask(0o077)
    case, = [c for c in orjson.loads((args.examples / 'human-judgments.json').read_bytes()) if c['case_id'] == args.case]
    records = []
    for r in case['original_files'].values():
        data = (args.examples / 'sources' / (r['sha256'] + '.json')).read_bytes()
        assert len(data) == r['bytes'] and hashlib.sha256(data).hexdigest() == r['sha256']
        node = orjson.loads(data)
        records.append((r, node, comparable(node), trajectory_id(parse_trajectory_record(node))))
    a, b = sorted(records, key=lambda x: len(x[2][0]))
    changes, lens = prove(a[2], b[2])
    change, = changes
    assert change['same_original_id']
    pos, idx = change['position'], change['call']
    identifier = a[2][1][pos][idx]
    base = dict(a[2][0][pos]['tool_calls'][idx]['function']['arguments'])
    base.pop('replace_all', None)
    member = orjson.loads(args.membership.read_bytes())
    assert member['passed']
    digest, lineages = hashlib.sha256(), {}
    with (args.publication / 'published/lineage.jsonl').open('rb') as stream:
        for line in stream:
            digest.update(line)
            row = orjson.loads(line)
            if row['trajectory_id'] in (a[3], b[3]):
                assert row['trajectory_id'] not in lineages
                lineages[row['trajectory_id']] = row['origins']
    assert digest.hexdigest() == member['lineage']['sha256'] and set(lineages) == {a[3], b[3]}
    owners, checks, times = defaultdict(set), {}, {}
    for side, identifier_key in (('short', a[3]), ('long', b[3])):
        for origin in lineages[identifier_key]:
            key, expected = origin['source_ref'], origin['sha256']
            owners[key].add(side)
            prior = checks.setdefault(key, expected)
            assert prior == expected
            times[key] = origin['captured_at']
    cfg = orjson.loads(args.config.read_bytes())
    client = _input_client(cfg['s3_endpoint'], cfg['s3_region'], cfg['s3_credentials_file'])
    loc = parse_s3_uri(args.raw_root)
    occurrences, totals, size = [], Counter(), 0
    for key in sorted(owners):
        response = client.get_object(Bucket=loc.bucket, Key=loc.key(key))
        with closing(response['Body']) as body:
            data = body.read()
        assert len(data) == response['ContentLength'] and hashlib.sha256(data).hexdigest() == checks[key]
        size += len(data)
        for part, values in observed_calls(orjson.loads(data), identifier):
            values = dict(values)
            if 'replace_all' not in values:
                state = 'omitted'
            elif values.pop('replace_all') is False:
                state = 'false'
            else:
                state = 'other'
            same = audit.dumps(values) == audit.dumps(base)
            totals[f'{part}/{state}/' + ('same_other_args' if same else 'different_other_args')] += 1
            occurrences.append({'source_ref': key, 'sha256': checks[key], 'owners': sorted(owners[key]),
                                'captured_at': times[key], 'part': part, 'state': state, 'same_other_args': same})
    report = {'input': member['output'], 'raw_root': args.raw_root, 'case': args.case,
              'short': a[0], 'long': b[0], 'visible_messages': lens, 'change': change,
              'id_sha256': hashlib.sha256(identifier.encode()).hexdigest(), 'lineage_sha256': digest.hexdigest(),
              'original_source_counts': [len(lineages[a[3]]), len(lineages[b[3]])],
              'shared_original_sources': sum(len(v) == 2 for v in owners.values()),
              'distinct_verified_raw_files': len(owners), 'verified_raw_bytes': size,
              'raw_occurrence_counts': totals, 'occurrences': occurrences}
    with args.output.open('xb') as out:
        out.write(orjson.dumps(report, option=orjson.OPT_INDENT_2) + b'\n')
    print(orjson.dumps({k: report[k] for k in ('visible_messages', 'original_source_counts',
                        'shared_original_sources', 'distinct_verified_raw_files', 'raw_occurrence_counts')}).decode())


if __name__ == '__main__':
    main()
