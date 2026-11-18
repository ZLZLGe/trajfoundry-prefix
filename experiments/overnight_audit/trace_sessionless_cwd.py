"""Resolve verified sessionless cwd pairs to the selected raw response and replay rows."""

import argparse
import hashlib
import os
from collections import Counter
from contextlib import closing
from itertools import islice
from pathlib import Path, PurePosixPath

import orjson
from survey_explicit_cwd import split_command
from trajfoundry.canonical import trajectory_id
from trajfoundry.models import Snapshot
from trajfoundry.output_contract import parse_trajectory_record, project_message
from trajfoundry.spark.adapters import parse_payload
from trajfoundry.spark.source import _input_client, _read_parquet_rows, parse_s3_uri
from verify_sessionless_cwd import candidate_pairs


def find_origins(client, normalized_uri, expected, nodes):
    wanted = {}
    for key, node in nodes.items():
        identifier = trajectory_id(parse_trajectory_record(node))
        metadata = node['metadata']
        assert identifier not in wanted
        wanted[identifier] = (key, metadata['source_file'], metadata['line_no'])
    loc = parse_s3_uri(normalized_uri)
    digest, found = hashlib.sha256(), {}
    response = client.get_object(Bucket=loc.bucket, Key=loc.key('lineage.jsonl'))
    with closing(response['Body']) as body:
        for line in body.iter_lines(chunk_size=4 * 1024**2):
            digest.update(line)
            digest.update(b'\n')
            row = orjson.loads(line)
            if row['trajectory_id'] not in wanted:
                continue
            key, name, index = wanted[row['trajectory_id']]
            tail = f'{name}#L{index:08d}'
            match, = [o for o in row['origins'] if PurePosixPath(o['source_ref']).name == tail]
            assert key not in found
            found[key] = match
    assert digest.hexdigest() == expected['sha256'] and response['ContentLength'] == expected['bytes']
    assert found.keys() == nodes.keys()
    return found


def raw_snapshot(client, location, origin):
    name, row = origin['source_ref'].rsplit('#L', 1)
    head = client.head_object(Bucket=location.bucket, Key=location.key(name))
    size, etag = head['ContentLength'], head['ETag']
    with closing(_read_parquet_rows(client, location, name, size, etag, max(size, 64 * 1024**2))) as rows:
        ref, data, digest = next(islice(rows, int(row), None))
    assert ref == origin['source_ref'] and digest == hashlib.sha256(data).hexdigest() == origin['sha256']
    parsed = parse_payload(ref, data, input_format='deepinfra')
    snap = Snapshot.model_validate_json(parsed.snapshot_json)
    messages = [project_message(m) for m in [*snap.history, *snap.response]]
    return messages, len(snap.history), etag


def locate_call(messages, identifier):
    (position, call), = (
        (i, c) for i, m in enumerate(messages) for c in (m.get('tool_calls') or ()) if c['id'] == identifier
    )
    assert call['function']['name'] == 'Bash'
    return position, call['function']['arguments']['command']


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--scan', type=Path, required=True)
    parser.add_argument('--verified', type=Path, required=True)
    parser.add_argument('--manifest-receipt', type=Path, required=True)
    parser.add_argument('--raw-root', required=True)
    parser.add_argument('--config', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    os.umask(0o077)
    cfg = orjson.loads(args.config.read_bytes())
    receipt = orjson.loads(args.manifest_receipt.read_bytes())
    client = _input_client(cfg['s3_endpoint'], cfg['s3_region'], cfg['s3_credentials_file'])
    loc = parse_s3_uri(args.raw_root)
    assert (args.verified / 'SUCCEEDED').is_file()
    with (args.verified / 'pairs.jsonl').open('rb') as stream:
        pairs = [r for r in map(orjson.loads, stream) if r['reason'] == 'verified']
    _, refs = candidate_pairs(args.scan)
    nodes = {}
    for key in sorted({r[k] for r in pairs for k in ('short', 'long')}):
        ref = refs[key]
        data = (args.verified / 'sources' / ref['sha256']).read_bytes()
        assert hashlib.sha256(data).hexdigest() == ref['sha256'] and len(data) == ref['bytes']
        nodes[key] = orjson.loads(data)
    origins = find_origins(client, receipt['input_uri'], receipt['lineage'], nodes)
    raw, objects = {}, {}
    for key, origin in origins.items():
        messages, history, etag = raw_snapshot(client, loc, origin)
        raw[key] = messages, history
        objects[key] = {'normalized_sha256': refs[key]['sha256'], 'origin': origin, 'etag': etag,
                        'history_messages': history, 'total_messages': len(messages),
                        'all_projected_messages_equal_normalized': messages == nodes[key]['messages']}
    results, counts, confirmed = [], Counter(), set()
    for pair in pairs:
        a, b, detail = pair['short'], pair['long'], pair['detail']
        assert detail['same_original_ids'] and not detail['binding_issues']
        terminal = next(m for m in reversed(nodes[a]['messages']) if m.get('tool_calls'))
        calls = []
        for k in detail['changed_call_positions']:
            identifier = terminal['tool_calls'][k]['id']
            ai, explicit = locate_call(raw[a][0], identifier)
            bi, replay = locate_call(raw[b][0], identifier)
            literal = split_command(explicit)
            calls.append({'call_index': k, 'id_sha256': hashlib.sha256(identifier.encode()).hexdigest(),
                          'left_position': ai, 'right_position': bi, 'left_is_response': ai >= raw[a][1],
                          'right_is_history': bi < raw[b][1],
                          'exact_command_suffix': literal is not None and literal[1] == replay})
        ok = all(c['left_is_response'] and c['right_is_history'] and c['exact_command_suffix'] for c in calls)
        if ok:
            confirmed.add(a)
        counts['response_to_history_pairs'] += ok
        results.append({'short': a, 'long': b, 'calls': calls})
    report = {'raw_root': args.raw_root, 'input': receipt['input_uri'], 'lineage_sha256': receipt['lineage']['sha256'],
              'source_records': len(objects), 'all_messages_match_normalized': sum(
                  obj['all_projected_messages_equal_normalized'] for obj in objects.values()),
              'pairs_checked': len(results), 'shorts_with_raw_replay': len(confirmed),
              'counts': counts, 'objects': objects, 'pairs': results}
    with args.output.open('xb') as out:
        out.write(orjson.dumps(report, option=orjson.OPT_INDENT_2) + b'\n')
    print(orjson.dumps({k: v for k, v in report.items() if k not in {'objects', 'pairs'}}).decode())


if __name__ == '__main__':
    main()
