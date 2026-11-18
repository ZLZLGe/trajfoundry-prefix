"""Trace shared cross-identity origins to both publication versions and the raw capture."""

import argparse
import hashlib
import os
from collections import Counter
from contextlib import closing
from pathlib import Path

import audit_core as audit
import orjson
from trajfoundry.canonical import trajectory_id
from trajfoundry.output_contract import parse_trajectory_record
from trajfoundry.spark.source import _input_client, parse_s3_uri
from verify_published_membership import checked_body


def collect_lineage(path, expected, wanted):
    digest, found = hashlib.sha256(), {}
    with path.open('rb') as stream:
        for line in stream:
            digest.update(line)
            row = orjson.loads(line)
            identifier = row['trajectory_id']
            if identifier in wanted:
                assert identifier not in found
                origins = {(o['source_ref'], o['sha256']) for o in row['origins']}
                assert len(origins) == row['origin_count']
                found[identifier] = origins
    assert digest.hexdigest() == expected['sha256'] and path.stat().st_size == expected['bytes']
    assert found.keys() == wanted
    return found


def manifest_members(path, sha, wanted):
    data = path.read_bytes()
    assert hashlib.sha256(data).hexdigest() == sha
    files = orjson.loads(data)['files']
    selected = {f['path']: f for f in files if f['path'] in wanted or f['path'] == 'lineage.jsonl'}
    lineage = selected.pop('lineage.jsonl')
    assert selected.keys() == wanted
    return lineage, selected


def main():
    p = argparse.ArgumentParser()
    p.add_argument('--comparison', type=Path, required=True)
    p.add_argument('--membership', type=Path, required=True)
    p.add_argument('--publication', type=Path, required=True)
    p.add_argument('--config', type=Path, required=True)
    p.add_argument('--raw-root', required=True)
    p.add_argument('--output', type=Path, required=True)
    args = p.parse_args()
    os.umask(0o077)
    parent = orjson.loads((args.comparison / 'report.json').read_bytes())
    member = orjson.loads(args.membership.read_bytes())
    assert parent['passed'] and (args.comparison / 'SUCCEEDED').is_file() and member['passed']
    assert parent['manifest_sha256'] == member['target_manifest_sha256']
    with (args.comparison / 'checks.jsonl').open('rb') as stream:
        pairs = [r for r in map(orjson.loads, stream) if r['shared_raw_origins']]
    assert len(pairs) == parent['counts']['all']['overlapping_sources']
    paths = {r[k] for r in pairs for k in ('short', 'long')}
    pub = args.publication / 'published'
    old_lineage, old = manifest_members(args.publication / 'source-manifest.json', member['source_manifest_sha256'], paths)
    new_lineage, new = manifest_members(pub / 'normalized-manifest.json', member['target_manifest_sha256'], paths)
    for path in paths:
        assert (old[path]['bytes'], old[path]['sha256']) == (new[path]['bytes'], new[path]['sha256'])
    cfg = orjson.loads(args.config.read_bytes())
    client = _input_client(cfg['s3_endpoint'], cfg['s3_region'], cfg['s3_credentials_file'])
    loc = parse_s3_uri(args.raw_root)
    records = {}
    for path, ref in new.items():
        data, _ = checked_body(client, member['output'] + path, ref['sha256'])
        assert len(data) == ref['bytes']
        node = orjson.loads(data)
        metadata = node['metadata']
        records[path] = (trajectory_id(parse_trajectory_record(node)), audit.ident(metadata.get('user_id')),
                         audit.ident(metadata.get('session_id')))
    wanted = {v[0] for v in records.values()}
    assert len(wanted) == len(records)
    before = collect_lineage(pub / 'source-lineage.jsonl', old_lineage, wanted)
    after = collect_lineage(pub / 'lineage.jsonl', new_lineage, wanted)
    counts, assignments, sessions = Counter(), Counter(), set()
    details, raw_ids = [], {}
    for r in pairs:
        ai, au, asession = records[r['short']]
        bi, bu, bsession = records[r['long']]
        assert au != bu and asession and asession == bsession
        sessions.add(asession)
        old_shared, shared = before[ai] & before[bi], after[ai] & after[bi]
        assert before[ai] <= after[ai] and before[bi] <= after[bi] and len(shared) == r['shared_raw_origins']
        counts['pairs_shared_in_old'] += bool(old_shared)
        counts['pairs_intersection_identical'] += old_shared == shared
        counts['pairs_with_added_shared_origins'] += bool(shared - old_shared)
        counts['pairs_both_entire_lineages_unchanged'] += before[ai] == after[ai] and before[bi] == after[bi]
        labels = Counter()
        for key in shared:
            name, sha = key
            if key not in raw_ids:
                response = client.get_object(Bucket=loc.bucket, Key=loc.key(name))
                with closing(response['Body']) as body:
                    data = body.read()
                assert len(data) == response['ContentLength'] and hashlib.sha256(data).hexdigest() == sha
                md = orjson.loads(data)['metadata']
                raw_ids[key] = (audit.ident(md.get('source_user_id')), audit.ident(md.get('session_id')))
            user, sid = raw_ids[key]
            group = 'missing' if not user else 'short_user' if user == au else 'long_user' if user == bu else 'other_user'
            labels[group] += 1
            assignments[group] += 1
            counts['raw_session_matches_both_per_pair_origin'] += sid == asession
        details.append({'short': r['short'], 'long': r['long'], 'old_shared': len(old_shared),
                        'new_shared': len(shared), 'newly_shared': len(shared - old_shared),
                        'old_counts': [len(before[ai]), len(before[bi])], 'new_counts': [len(after[ai]), len(after[bi])],
                        'raw_user_relationships': labels})
    result = {'passed': True, 'source_uri': member['input'], 'target_uri': member['output'], 'raw_root': args.raw_root,
              'lineage_sha256_before': old_lineage['sha256'], 'lineage_sha256_after': new_lineage['sha256'],
              'pairs': len(pairs), 'normalized_files': len(records), 'distinct_sessions': len(sessions),
              'distinct_raw_origins_verified': len(raw_ids), 'counts': counts, 'pair_origin_user_assignments': assignments,
              'details': details}
    with args.output.open('xb') as dst:
        dst.write(orjson.dumps(result, option=orjson.OPT_INDENT_2) + b'\n')
    print(orjson.dumps({k: v for k, v in result.items() if k != 'details'}).decode())


if __name__ == '__main__':
    main()
