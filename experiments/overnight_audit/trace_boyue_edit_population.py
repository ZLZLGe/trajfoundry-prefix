"""Find a raw response and a raw request replay for each distinct shorter Edit-default witness."""

import argparse
import hashlib
import os
from collections import Counter, defaultdict
from contextlib import closing
from datetime import datetime
from functools import lru_cache
from pathlib import Path

import audit_core as audit
import orjson
from trace_boyue_edit_default import observed_calls
from trajfoundry.canonical import trajectory_id
from trajfoundry.output_contract import parse_trajectory_record
from trajfoundry.spark.source import _input_client, parse_s3_uri
from verify_edit_defaults import comparable, prove


def find_source(origins, created, part, desired, load):
    # The representative capture normally shares the normalized timestamp. For ties and
    # other captures, require every expected call and argument in the same original object.
    ordered = sorted(origins, key=lambda o: (o['captured_at'] != created, o['captured_at'], o['source_ref']))
    for origin in ordered:
        raw = load(origin['source_ref'], origin['sha256'])
        for identifier, expected in desired:
            calls = [audit.dumps(args) for where, args in observed_calls(raw, identifier) if where == part]
            if calls != [expected]:
                break
        else:
            return {k: origin[k] for k in ('source_ref', 'sha256', 'captured_at')}
    return None


def main():
    p = argparse.ArgumentParser()
    p.add_argument('--verified', required=True, type=Path)
    p.add_argument('--membership', required=True, type=Path)
    p.add_argument('--publication', required=True, type=Path)
    p.add_argument('--raw-root', required=True)
    p.add_argument('--config', required=True, type=Path)
    p.add_argument('--output', required=True, type=Path)
    args = p.parse_args()
    os.umask(0o077)
    verify = orjson.loads((args.verified / 'report.json').read_bytes())
    member = orjson.loads(args.membership.read_bytes())
    assert verify['passed'] and member['passed'] and verify['manifest_sha256'] == member['target_manifest_sha256']
    options = defaultdict(list)
    with (args.verified / 'witnesses.jsonl').open('rb') as stream:
        for w in map(orjson.loads, stream):
            if w['kind'] == 'strict' and not w['isolation']:
                options[w['short']].append(w)
    assert len(options) == verify['nonisolated_strict_shorter']
    subset = [min((w for w in ws if w['long'] not in options), key=lambda w: (w['lengths'][1], w['long']))
              for _, ws in sorted(options.items())]
    paths = {w[k] for w in subset for k in ('short', 'long')}
    manifest = (args.publication / 'published/normalized-manifest.json').read_bytes()
    assert hashlib.sha256(manifest).hexdigest() == member['target_manifest_sha256']
    refs = {r['path']: r for r in orjson.loads(manifest)['files'] if r['path'] in paths}
    assert refs.keys() == paths

    @lru_cache(maxsize=16)
    def node(key):
        meta = refs[key]
        data = (args.verified / 'sources' / meta['sha256']).read_bytes()
        assert len(data) == meta['bytes'] and hashlib.sha256(data).hexdigest() == meta['sha256']
        return orjson.loads(data)

    identifiers = {trajectory_id(parse_trajectory_record(node(k))): k for k in sorted(paths)}
    assert len(identifiers) == len(paths)
    origins, digest = {}, hashlib.sha256()
    with (args.publication / 'published/lineage.jsonl').open('rb') as stream:
        for line in stream:
            digest.update(line)
            row = orjson.loads(line)
            key = identifiers.get(row['trajectory_id'])
            if key:
                assert key not in origins
                origins[key] = row['origins']
    assert digest.hexdigest() == member['lineage']['sha256'] and origins.keys() == paths
    location = parse_s3_uri(args.raw_root)
    cfg = orjson.loads(args.config.read_bytes())
    client = _input_client(cfg['s3_endpoint'], cfg['s3_region'], cfg['s3_credentials_file'])
    raw_sizes = {}

    @lru_cache(maxsize=24)
    def raw(key, sha):
        response = client.get_object(Bucket=location.bucket, Key=location.key(key))
        with closing(response['Body']) as stream:
            data = stream.read()
        assert len(data) == response['ContentLength'] and hashlib.sha256(data).hexdigest() == sha
        raw_sizes[key, sha] = len(data)
        return orjson.loads(data)

    counts, cases, sessions = Counter(), [], set()
    for w in subset:
        a, b = (node(w[k]) for k in ('short', 'long'))
        left, right = comparable(a), comparable(b)
        changes, lengths = prove(left, right)
        assert changes == w['changes'] and lengths == w['lengths']
        session = audit.ident(a['metadata'].get('session_id'))
        assert session and session == audit.ident(b['metadata'].get('session_id'))
        sessions.add(session)
        desired = [[], []]
        for c in changes:
            i, k = c['position'], c['call']
            assert i == lengths[0] - 1 and c['false_on'] == 'long' and c['same_original_id']
            for side, item in enumerate((left, right)):
                messages, ids = item
                desired[side].append((ids[i][k], audit.dumps(messages[i]['tool_calls'][k]['function']['arguments'])))
        early = find_source(origins[w['short']], a['metadata']['created_at'], 'response', desired[0], raw)
        later = find_source(origins[w['long']], b['metadata']['created_at'], 'request_history', desired[1], raw)
        counts['response_found'] += bool(early)
        counts['history_found'] += bool(later)
        counts['both_found'] += bool(early and later)
        if early and later:
            delta = (datetime.fromisoformat(later['captured_at']) - datetime.fromisoformat(early['captured_at'])).total_seconds()
            counts['later_capture_time'] += delta > 0
            counts['same_capture_time'] += delta == 0
            counts['earlier_capture_time'] += delta < 0
        else:
            delta = None
        cases.append({'short': w['short'], 'long': w['long'], 'lengths': lengths,
                      'changed_calls': len(changes), 'original_call_id_sha256': [
                          hashlib.sha256(i.encode()).hexdigest() for i, _ in desired[0]],
                      'raw_response': early, 'raw_request_history': later, 'capture_gap_seconds': delta})
    result = {'passed': True, 'uri': member['output'], 'raw_root': args.raw_root, 'lineage_sha256': digest.hexdigest(),
              'cases_checked': len(cases), 'sessions': len(sessions), 'raw_evidence': counts,
              'distinct_published_files': len(paths), 'unique_raw_files_read': len(raw_sizes),
              'unique_raw_bytes_read': sum(raw_sizes.values()), 'cases': cases,
              'selection': 'One direct longer trajectory outside the shorter set per distinct short, ordered by '
                           'message length then path. Report the first exact typed tool invocation found in the '
                           'raw response or request history, preferring each representative capture timestamp.'}
    with args.output.open('xb') as stream:
        stream.write(orjson.dumps(result, option=orjson.OPT_INDENT_2) + b'\n')
    print(orjson.dumps({k: v for k, v in result.items() if k not in {'cases', 'selection'}}).decode())


if __name__ == '__main__':
    main()
