"""Trace selected replacement-character prefix witnesses to their logical masked-raw rows."""

import argparse
import hashlib
import json
import os
from contextlib import closing
from itertools import islice
from pathlib import Path

import audit_core as audit
from explain_family_replacement_characters import characters
from trajfoundry.models import Snapshot
from trajfoundry.output_contract import project_message
from trajfoundry.spark.adapters import parse_payload
from trajfoundry.spark.source import _input_client, _read_parquet_rows, parse_s3_uri


def strings(value, path=()):
    if isinstance(value, str):
        yield path, value
    elif isinstance(value, list):
        for i, item in enumerate(value):
            yield from strings(item, (*path, i))
    elif isinstance(value, dict):
        for key, item in value.items():
            yield from strings(item, (*path, key))


def trace(sub, row, positions, family, client, location):
    source = (family / 'sources' / row['ref']['sha256']).read_bytes()
    assert len(source) == row['ref']['bytes'] and hashlib.sha256(source).hexdigest() == row['ref']['sha256']
    node = json.loads(source)
    name, index = (node['metadata'][k] for k in ('source_file', 'line_no'))
    head = client.head_object(Bucket=location.bucket, Key=location.key(name))
    size = head['ContentLength']
    with closing(_read_parquet_rows(client, location, name, size, head['ETag'], max(size, 64 * 1024**2))) as rows:
        ref, payload, digest = next(islice(rows, index, None))
    assert ref == f'{name}#L{index:08d}' and hashlib.sha256(payload).hexdigest() == digest
    raw = json.loads(payload)
    request = json.loads(raw['request']['body'])
    parsed = parse_payload(ref, payload, input_format='deepinfra')
    snapshot = Snapshot.model_validate_json(parsed.snapshot_json)
    assert snapshot.session_id == node['metadata']['session_id'] and snapshot.model == node['model']
    normalized = node['messages']
    projected = [project_message(m) for m in [*snapshot.history, *snapshot.response]]
    stable, _ = audit.stable_messages(node)
    assert len(projected) == len(stable) == len(normalized)
    proofs = []
    for change in positions:
        pos, folded_index = change['position'], change['folded_index']
        text = normalized[pos]['content']
        assert pos < len(snapshot.history) and projected[pos] == normalized[pos] and stable[pos]['content'] == text
        _, widths = characters(text)
        offset = folded_index + sum(w - 1 for i, w in widths.items() if i < folded_index)
        width = widths.get(folded_index, 1)
        assert text[offset:offset + width] == change['expected']
        window = text[max(0, offset - 20):offset + width + 20]
        hits = [{'path': path, 'field_sha256': hashlib.sha256(value.encode()).hexdigest(),
                 'field_contains_entire_normalized_text': text in value}
                for path, value in strings(request['messages'], ('messages',)) if window in value]
        if not hits:
            raise ValueError(f'No supporting request text at the cited location for sub {sub} message {pos}')
        proofs.append({'normalized_message': pos, 'role': normalized[pos]['role'], 'change': change['expected'],
                       'excerpt': text[max(0, offset - 3):offset + width + 3], 'raw_matches': hits})
    return {
        'sub': sub, 'normalized_sha256': row['ref']['sha256'], 'source_key': location.key(name), 'row': index,
        'parquet_etag': head['ETag'], 'raw_json_sha256': digest, 'parsed_request_messages': len(snapshot.history),
        'parsed_response_messages': len(snapshot.response), 'normalized_messages': len(normalized),
        'all_message_roles_and_contents_match': all((a['role'], a['content']) == (b['role'], b['content'])
                                                    for a, b in zip(normalized, projected)),
        'entire_message_objects_match': projected == normalized, 'proof': proofs,
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--family', type=Path, required=True)
    parser.add_argument('--replacement', type=Path, required=True)
    parser.add_argument('--raw-root', required=True)
    parser.add_argument('--config', type=Path, required=True)
    parser.add_argument('--pair', nargs=2, type=int, action='append', required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    os.umask(0o077)
    pairs = {tuple(pair) for pair in args.pair}
    positions = {}
    seen = set()
    for row in map(json.loads, (args.replacement / 'witnesses.jsonl').open()):
        pair = row['left_sub'], row['right_sub']
        if pair not in pairs:
            continue
        assert pair not in seen and row['kind'] == 'strict_prefix'
        seen.add(pair)
        for msg in row['differing_messages']:
            for side, idx, width, observed in msg['edits']:
                for label, sub in zip(('left', 'right'), pair):
                    positions.setdefault(sub, []).append({
                        'position': msg['position'], 'folded_index': idx,
                        'expected': '\ufffd' * width if side == label else observed,
                    })
    assert seen == pairs
    config = json.loads(args.config.read_bytes())
    client = _input_client(config['s3_endpoint'], config['s3_region'], config['s3_credentials_file'])
    location = parse_s3_uri(args.raw_root)
    results = [trace(row['sub'], row, positions[row['sub']], args.family, client, location)
               for row in map(json.loads, (args.family / 'family.jsonl').open()) if row['sub'] in positions]
    assert {row['sub'] for row in results} == positions.keys()
    report = {'pairs': sorted(pairs), 'all_edit_windows_found_in_provider_requests': True, 'records': results}
    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open('x', encoding='utf-8') as stream:
        json.dump(report, stream, ensure_ascii=False, indent=2)
        stream.write('\n')
    print(f'{len(results)} logical raw rows verified for {len(pairs)} prefix witnesses')


if __name__ == '__main__':
    main()
