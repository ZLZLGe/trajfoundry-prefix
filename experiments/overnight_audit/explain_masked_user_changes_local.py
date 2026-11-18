"""Read every masking removal with changed user messages, without merging.

The narrow timezone diagnostic accepts only an exact Shanghai spelling change.
Everything else remains available for human review, with complete source hashes.
"""

import argparse
import collections
import hashlib
import json
import os
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import audit_core as audit

from trajectory_prefix.storage import Reader, StorageConfig


def timezone_view(text):
    return text.replace('[Asia/<LOCATION>]', '[Asia/Shanghai]')


def user_change(index, left, right):
    start = 0
    while start < min(len(left), len(right)) and left[start] == right[start]:
        start += 1
    end = 0
    while end < min(len(left), len(right)) - start and left[-end - 1] == right[-end - 1]:
        end += 1
    spans = [text[start:len(text) - end] for text in (left, right)]
    return {
        'message_index': index,
        'lengths': [len(left), len(right)],
        'common_prefix_chars': start,
        'common_suffix_chars': end,
        'changed_span_lengths': list(map(len, spans)),
        'changed_span_sha256': [hashlib.sha256(text.encode()).hexdigest() for text in spans],
        'changed_span_excerpts': [audit.safe_excerpt(text, 2400) for text in spans],
        'context_excerpts': [audit.safe_excerpt(text[max(0, start - 300):start + 2500], 2800)
                             for text in (left, right)],
        'classification': ('exact_shanghai_timezone_spelling_only'
                           if timezone_view(left) == timezone_view(right) else 'other_user_text'),
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--publication', type=Path, required=True)
    parser.add_argument('--config', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    os.umask(0o077)
    args.output.mkdir(parents=True, exist_ok=False)
    population = []
    for channel in ('free-router', 'boyue-rich', 'token-plan'):
        root = args.publication / channel / 'refined'
        report = json.loads((root / 'report.json').read_text())
        assert (root / 'SUCCEEDED').is_file() and report['passed']
        rows = [json.loads(line) for part in sorted((root / 'explanations').glob('part-*'))
                for line in part.open()]
        assert len(rows) == report['mask_additional_removed']
        population.extend(rows)
    wanted = sorted((row for row in population
                     if any(d['role'] == 'user' for d in row['differences'])),
                    key=lambda row: row['record']['id'])
    config = json.loads(args.config.read_text())
    storage = StorageConfig(endpoint_url=config['s3_endpoint'], region=config['s3_region'],
                            credentials_file=config['s3_credentials_file'],
                            cache_dir=str(args.output / 'sources'))
    local = threading.local()

    def read(row):
        if not hasattr(local, 'reader'):
            local.reader = Reader(storage)
        nodes = [local.reader.node(row[key]) for key in ('record', 'retained')]
        assert audit.scope(nodes[0], row['record']) == audit.scope(nodes[1], row['retained'])
        messages = [audit.stable_messages(node)[0] for node in nodes]
        assert list(map(len, messages)) == row['message_counts']
        changes = []
        for index, (left, right) in enumerate(zip(*messages)):
            assert left['role'] == right['role']
            if left['role'] == 'user' and left != right:
                assert {k: v for k, v in left.items() if k != 'content'} == {
                    k: v for k, v in right.items() if k != 'content'}
                changes.append(user_change(index, left['content'], right['content']))
        expected = {d['message_index'] for d in row['differences'] if d['role'] == 'user'}
        assert {change['message_index'] for change in changes} == expected
        assert changes
        only_timezone = all(change['classification'] == 'exact_shanghai_timezone_spelling_only'
                            for change in changes)
        return {
            'record': row['record'], 'retained': row['retained'], 'model': row['model'],
            'has_session': row['has_session'], 'message_counts': row['message_counts'],
            'user_change_class': ('exact_shanghai_timezone_spelling_only'
                                  if only_timezone else 'contains_other_user_text'),
            'user_changes': changes,
            'other_changed_roles': sorted({d['role'] for d in row['differences']
                                          if d['role'] != 'user'}),
            'necessary_labels': row['necessary_labels'], 'source_hashes_verified': True,
            'automatic_semantic_verdict': None,
        }

    counts, field_counts, role_counts = collections.Counter(), collections.Counter(), collections.Counter()
    refs, other, finished = {}, [], 0
    started = time.monotonic()
    with ThreadPoolExecutor(max_workers=6) as pool, (args.output / 'pairs.jsonl').open('w') as out:
        for result in pool.map(read, wanted):
            out.write(json.dumps(result, ensure_ascii=False) + '\n')
            dataset = result['record']['dataset']
            counts[dataset + '/' + result['user_change_class']] += 1
            for change in result['user_changes']:
                field_counts[dataset + '/' + change['classification']] += 1
            role_counts[dataset + '/' + ','.join(result['other_changed_roles'])] += 1
            if result['user_change_class'] == 'contains_other_user_text':
                other.append(result)
            for key in ('record', 'retained'):
                ref = result[key]
                refs[ref['sha256']] = ref['bytes']
            finished += 1
            if finished % 200 == 0:
                out.flush()
                print(json.dumps({'event': 'progress', 'pairs': finished, 'total': len(wanted),
                                  'elapsed_seconds': round(time.monotonic() - started, 2)}), flush=True)
    report = {
        'passed': True, 'local_only': True, 's3_writes': False,
        'all_mask_removals': len(population), 'pairs_with_changed_user_text': len(wanted),
        'complete_pairs_read': finished, 'distinct_source_objects': len(refs),
        'distinct_source_bytes': sum(refs.values()), 'pair_counts': dict(counts),
        'user_message_counts': dict(field_counts), 'other_changed_role_sets': dict(role_counts),
        'other_user_text_pairs': len(other),
        'limitation': ('This classifies only user-text changes. Other roles can also differ; '
                       'neither class is an automatic semantic correctness verdict.'),
    }
    (args.output / 'other-user-text-pairs.json').write_text(json.dumps(other, ensure_ascii=False, indent=2) + '\n')
    (args.output / 'report.json').write_text(json.dumps(report, ensure_ascii=False, indent=2) + '\n')
    (args.output / 'SUCCEEDED').write_text('All selected source objects read and SHA256 verified locally.\n')
    print(json.dumps(report, ensure_ascii=False), flush=True)


if __name__ == '__main__':
    main()
