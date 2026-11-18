"""Check replacement-character prefix witnesses within every repeated filename family of a verified plan."""

import argparse
import hashlib
import os
import threading
from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import audit_core as audit
import orjson
from explain_family_progress_requests import FORK, PROMPT
from explain_family_replacement_characters import BROKEN, compare_family
from review_largest_family_local import records

from trajectory_prefix.storage import Reader, StorageConfig


def checked_families(refined):
    plan = refined / 'final-input.jsonl'
    check = orjson.loads((refined / 'ordinary-final-recheck/report.json').read_bytes())
    with plan.open('rb') as stream:
        digest = hashlib.file_digest(stream, 'sha256').hexdigest()
    assert check['check_only'] and check['removed_count'] == 0 and digest == check['input_plan_sha256']
    counts = Counter(family for _, family, _ in records(plan))
    assert counts.total() == check['kept_count']
    families = defaultdict(list)
    for ref, family, sub in records(plan):
        if counts[family] > 1:
            families[family].append((sub, ref))
    for family, rows in families.items():
        assert len(rows) == len({sub for sub, _ in rows}) == counts[family]
    return sorted(families.items(), key=lambda fr: (-len(fr[1]), fr[0])), digest, counts.total()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--refined', type=Path, required=True)
    parser.add_argument('--config', type=Path, required=True)
    parser.add_argument('--prior-cache', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    os.umask(0o077)
    families, plan_digest, plan_size = checked_families(args.refined)
    args.output.mkdir(parents=True, exist_ok=False)
    cache = args.output / 'sources'
    config = orjson.loads(args.config.read_bytes())
    storage = StorageConfig(endpoint_url=config['s3_endpoint'], region=config['s3_region'],
                            credentials_file=config['s3_credentials_file'], cache_dir=str(cache))
    local = threading.local()

    def source_path(ref):
        existing = args.prior_cache / ref['sha256']
        return existing if existing.is_file() else cache / ref['sha256']

    def fetch(ref):
        if source_path(ref).is_file():
            return
        if not hasattr(local, 'reader'):
            local.reader = Reader(storage)
        local.reader.read(ref)

    totals, pair_counts = Counter(), Counter()
    with (ThreadPoolExecutor(max_workers=6) as pool, (args.output / 'families.jsonl').open('wb') as family_out,
          (args.output / 'members.jsonl').open('wb') as member_out, (args.output / 'witnesses.jsonl').open('wb') as out):
        for i, (family, entries) in enumerate(families, start=1):
            for _ in pool.map(fetch, (ref for _, ref in entries)):
                pass
            census = Counter()
            scopes, models = Counter(), Counter()

            def members():
                for sub, ref in sorted(entries):
                    data = source_path(ref).read_bytes()
                    assert len(data) == ref['bytes'] and hashlib.sha256(data).hexdigest() == ref['sha256']
                    node = orjson.loads(data)
                    scope = hashlib.sha256(audit.dumps(audit.scope(node, ref))).hexdigest()
                    scopes[scope] += 1
                    models[node['model']] += 1
                    last_user = next((m for m in reversed(node['messages']) if m['role'] == 'user'), {})
                    census['members'] += 1
                    census['source_bytes'] += len(data)
                    census['session_matches_filename'] += (node['metadata'].get('session_id') == family)
                    census['last_user_starts_progress_prompt'] += last_user.get('content', '').startswith(PROMPT)
                    census['user_history_contains_fork'] += any(
                        m['role'] == 'user' and m['content'].startswith(FORK) for m in node['messages'])
                    has_damage = any(BROKEN in m['content'] for m in node['messages'])
                    census['raw_messages_contain_replacement'] += has_damage
                    member_out.write(orjson.dumps({'family': family, 'sub': sub, 'ref': ref, 'scope_hash': scope,
                                                  'raw_has_damage': has_damage}) + b'\n')
                    yield sub, scope, node

            def write_witness(witness):
                out.write(orjson.dumps({'family': family, **witness}) + b'\n')

            summary = compare_family(members(), write_witness)
            assert census['members'] == summary['cached_sources_reread'] == len(entries)
            combined = {**census, 'scopes': len(scopes),
                        'families_with_damage': int(summary['records_with_replacement_in_visible_content'] > 0),
                        'families_with_strict_pairs': int(summary['distinct_strict_shorter_records'] > 0),
                        'families_with_equal_pairs': int(summary['distinct_equal_length_members'] > 0),
                        'families_with_progress_prompt': int(census['last_user_starts_progress_prompt'] > 0),
                        **{k: summary[k] for k in ('records_with_replacement_in_visible_content',
                           'distinct_records_in_verified_pairs', 'distinct_strict_shorter_records',
                           'distinct_equal_length_members')}}
            totals.update(combined)
            pair_counts.update(summary['pair_counts'])
            family_out.write(orjson.dumps({'family': family, 'models': models, 'counts': combined,
                                           'pair_counts': summary['pair_counts']}) + b'\n')
            if i == 1 or i % 100 == 0:
                print(f'families={i}/{len(families)} sources={totals["members"]}', flush=True)
    report = {'passed': True, 'local_only': True, 'plan_sha256': plan_digest, 'plan_count': plan_size,
              'repeated_filename_families': len(families), 'counts': totals, 'pair_counts': pair_counts,
              'limit': 'Only same filename-family pairs within the same independent identity scope are compared.'}
    (args.output / 'report.json').write_bytes(orjson.dumps(report, option=orjson.OPT_INDENT_2) + b'\n')
    (args.output / 'SUCCEEDED').write_text('Every repeated filename family compared. All member SHA256s verified.\n')
    print(orjson.dumps(report).decode(), flush=True)


if __name__ == '__main__':
    main()
