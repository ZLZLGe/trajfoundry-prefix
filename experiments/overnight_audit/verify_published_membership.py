"""Independently join a live S3 publication manifest to its retained plan and read its lineage."""

import argparse
import hashlib
import os
from collections import defaultdict
from contextlib import closing
from pathlib import Path
from urllib.parse import urlsplit

import boto3
import orjson
from botocore.config import Config

from trajectory_prefix.storage import credentials, file_sha256


def get(client, uri):
    parsed = urlsplit(uri)
    assert parsed.scheme == 's3' and parsed.netloc and not parsed.query and not parsed.fragment
    return client.get_object(Bucket=parsed.netloc, Key=parsed.path.lstrip('/'))


def checked_body(client, uri, expected_sha):
    obj = get(client, uri)
    with closing(obj['Body']) as body:
        data = body.read()
    assert len(data) == obj['ContentLength'] and hashlib.sha256(data).hexdigest() == expected_sha
    return data, obj['ETag']


def consume_plan(files, path, anchors):
    entries = {f['path']: (f['bytes'], f['sha256']) for f in files}
    assert len(entries) == len(files), 'Duplicate published manifest path'
    lineage = entries.pop('lineage.jsonl')
    for anchor in anchors:
        keep = anchor['retained']
        assert anchor['removed']['path'] not in entries and entries[keep['path']] == (keep['bytes'], keep['sha256'])
    digest = hashlib.sha256()
    size = count = 0
    with path.open('rb') as stream:
        for line in stream:
            digest.update(line)
            r = orjson.loads(line)
            assert entries.pop(r['path']) == (r['bytes'], r['sha256']), 'Retained ref differs from manifest'
            count += 1
            size += r['bytes']
    assert not entries, 'Manifest contains unexpected retained records'
    return count, size, digest.hexdigest(), lineage


def read_lineage(body, lineage, checks):
    digest = hashlib.sha256()
    rows, total, origin_instances = 0, 0, 0
    identifiers, distinct_origins = set(), set()
    found = {name: False for name in checks}
    by_id = defaultdict(list)
    origin_to_checks = defaultdict(set)
    other_rows = {name: 0 for name in checks}
    for name, (identifier, origins) in checks.items():
        by_id[identifier].append(name)
        for origin in origins:
            origin_to_checks[origin].add(name)
    pending = b''
    for chunk in body.iter_chunks(chunk_size=4 * 1024**2):
        total += len(chunk)
        digest.update(chunk)
        lines = (pending + chunk).split(b'\n')
        pending = lines.pop()
        for line in lines:
            row = orjson.loads(line)
            identifier = row['trajectory_id']
            assert identifier not in identifiers
            identifiers.add(identifier)
            origins = {(o['source_ref'], o['sha256']) for o in row['origins']}
            assert len(origins) == row['origin_count'] == len(row['origins']) and origins
            other_checks = set()
            for ref, sha in origins:
                # Compare the same immutable (relative path, content hash) identity across all rows.
                distinct_origins.add(hashlib.sha256(ref.encode() + b'\x00' + bytes.fromhex(sha)).digest())
                for name in origin_to_checks.get((ref, sha), ()):
                    if identifier != checks[name][0]:
                        other_checks.add(name)
            for name in other_checks:
                other_rows[name] += 1
            for name in by_id.get(identifier, ()):
                assert checks[name][1] <= origins and not found[name]
                found[name] = True
            origin_instances += len(origins)
            rows += 1
    assert not pending and (total, digest.hexdigest()) == lineage and all(found.values())
    return {'rows': rows, 'bytes': total, 'sha256': digest.hexdigest(),
            'origin_instances': origin_instances, 'distinct_origins': len(distinct_origins),
            'located_anchors': found, 'other_rows_sharing_anchor_origins': other_rows}


def verify(args):
    root = args.channel_root
    anchors = orjson.loads(args.anchors.read_bytes()) if args.anchors else []
    if anchors:
        from trajfoundry.canonical import trajectory_id
        from trajfoundry.output_contract import parse_trajectory_record
    source_plan = orjson.loads((root / 'plan-complete.json').read_bytes())
    pub = orjson.loads((root / 'published/report.json').read_bytes())
    refined = orjson.loads((root / 'refined/report.json').read_bytes())
    ordinary = orjson.loads((root / 'baseline/report.json').read_bytes())
    zero_check = orjson.loads((root / 'refined/ordinary-final-recheck/report.json').read_bytes())
    assert (root / 'published/SUCCEEDED').is_file() and (root / 'refined/SUCCEEDED').is_file()
    assert any(orjson.loads(p.read_bytes()).get('passed') for p in root.glob('control-publish*/final.json'))
    assert pub['passed'] and refined['passed'] and source_plan['complete_requested_prefix']
    assert refined['verification']['ordinary_recheck_matches'] == refined['verification']['retained_recheck_matches'] == 0
    cfg = orjson.loads(args.config.read_bytes())
    client = boto3.client('s3', endpoint_url=cfg['s3_endpoint'], region_name=cfg['s3_region'],
                          config=Config(retries={'mode': 'standard', 'max_attempts': 6}),
                          **credentials(cfg['s3_credentials_file']))
    roots = (pub['source_uri'], pub['normalized_uri'])
    assert roots == (source_plan['source_uri'], source_plan['normalized_uri'])
    source_sha = source_plan['source_manifest_sha256']
    data, etag = checked_body(client, roots[0] + 'manifest.json', source_sha)
    assert etag == source_plan['manifest_etag']
    source = orjson.loads(data)
    count_a = source['record_count']
    assert source['status'] == 'SUCCEEDED' and count_a == source_plan['input_count'] == pub['input_count'] == refined['input_count']
    del source, data
    target_sha = file_sha256(root / 'published/normalized-manifest.json')
    data, target_etag = checked_body(client, roots[1] + 'manifest.json', target_sha)
    manifest = orjson.loads(data)
    del data
    count_c, size, plan_sha, lineage = consume_plan(manifest.pop('files'), root / 'refined/final-input.jsonl', anchors)
    assert plan_sha == zero_check['input_plan_sha256'] and (not args.expected_plan_sha or args.expected_plan_sha == plan_sha)
    assert zero_check['removed_count'] == 0 and count_c == zero_check['kept_count']
    assert manifest['status'] == 'SUCCEEDED' and count_c == manifest['record_count'] == pub['kept_count'] == refined['kept_count']
    assert size == manifest['total_bytes'] == pub['normalized_bytes']
    count_b = ordinary['kept_count']
    assert ordinary['input_count'] == count_a and count_b == count_a - refined['baseline_removed']
    assert count_a - count_c == refined['removed_count'] == pub['removed_count']

    checks = {}
    for anchor in anchors:
        keep = anchor['retained']
        raw, _ = checked_body(client, roots[1] + keep['path'], keep['sha256'])
        assert len(raw) == keep['bytes'] and anchor['name'] not in checks
        checks[anchor['name']] = (trajectory_id(parse_trajectory_record(orjson.loads(raw))),
                                  {(o['source_ref'], o['sha256']) for o in anchor['origins']})
        assert len(checks[anchor['name']][1]) >= 2
    live = get(client, roots[1] + 'lineage.jsonl')
    with closing(live['Body']) as stream:
        lineage_result = read_lineage(stream, lineage, checks)
    assert count_c == lineage_result['rows'] == pub['verification']['lineage_rows']
    assert pub['verification']['unique_raw_sources'] == lineage_result['distinct_origins']
    return {'passed': True, 'channel': root.name, 'input': roots[0], 'output': roots[1],
            'counts': {'A': count_a, 'B': count_b, 'C': count_c}, 'source_manifest_sha256': source_sha,
            'target_manifest_sha256': target_sha, 'target_manifest_etag': target_etag,
            'ordinary_rechecked_plan_sha256': plan_sha, 'all_published_member_references_match': True,
            'trajectory_bytes': size, 'lineage': lineage_result}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--channel-root', type=Path, required=True)
    parser.add_argument('--config', type=Path, required=True)
    parser.add_argument('--anchors', type=Path)
    parser.add_argument('--expected-plan-sha')
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    os.umask(0o077)
    report = verify(args)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open('xb') as file:
        file.write(orjson.dumps(report, option=orjson.OPT_INDENT_2) + b'\n')
    print(orjson.dumps({'channel': report['channel'], 'counts': report['counts'], 'lineage': report['lineage']}).decode())


if __name__ == '__main__':
    main()
