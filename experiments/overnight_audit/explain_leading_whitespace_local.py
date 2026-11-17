"""Distinguish leading line breaks, blank lines, and indentation locally.

These are diagnostic comparisons of already discovered candidates, not new
production merge rules. Every reported relation covers the complete history.
"""
import argparse
import collections
import json
import os
import re
import threading
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import audit_core as audit

from trajectory_prefix.storage import Reader, StorageConfig

BLANK_LINES = re.compile(r'\A(?:[ \t]*(?:\r\n|\n|\r))+')


def explain(row, nodes):
    messages = [audit.stable_messages(node)[0] for node in nodes]
    assert audit.scope(nodes[0], row['short']) == audit.scope(nodes[1], row['long'])
    assert row['eligible_same_scope'] and not row['production_raw_covers']
    assert messages[0] and len(messages[0]) <= len(messages[1])
    changes = []
    newline_only = blank_lines_only = exact_two_newlines = True
    for index, (left, right) in enumerate(zip(*messages)):
        if audit.dumps(left) == audit.dumps(right):
            continue
        assert left['role'] == right['role'] == 'assistant'
        assert audit.dumps({k: v for k, v in left.items() if k != 'content'}) == audit.dumps(
            {k: v for k, v in right.items() if k != 'content'})
        a, b = left['content'], right['content']
        assert a.lstrip(' \t\r\n') == b.lstrip(' \t\r\n')
        newline_only &= a.lstrip('\r\n') == b.lstrip('\r\n')
        blank_lines_only &= BLANK_LINES.sub('', a) == BLANK_LINES.sub('', b)
        exact_two_newlines &= a == '\n\n' + b or b == '\n\n' + a
        # Only whitespace is included in this evidence; no response text.
        changes.append({'message_index': index,
                        'prefixes': [s[:len(s) - len(s.lstrip(' \t\r\n'))] for s in (a, b)]})
    assert changes
    assert not newline_only or blank_lines_only
    category = ('line_breaks_only' if newline_only else
                'blank_lines_with_whitespace' if blank_lines_only else
                'requires_stripping_space_or_tab_on_content_line')
    return {'short': row['short'], 'long': row['long'], 'model': row['model'],
            'category': category, 'changed_messages': changes,
            'only_final_assistant_differs': len(changes) == 1 and
            changes[0]['message_index'] == len(messages[0]) - 1,
            'all_changes_exactly_two_newlines': exact_two_newlines,
            'full_prefix_after_stripping_only_line_breaks': newline_only,
            'full_prefix_after_removing_only_blank_lines': blank_lines_only,
            'full_prefix_after_stripping_leading_ascii_whitespace': True,
            'full_objects_sha256_verified': True, 'diagnostic_only': True}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--attribution', type=Path, required=True)
    parser.add_argument('--config', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    os.umask(0o077)
    assert (args.attribution / 'SUCCEEDED').is_file()
    source_report = json.loads((args.attribution / 'report.json').read_text())
    assert source_report['passed']
    rows = []
    for part in sorted((args.attribution / 'attributed').glob('part-*')):
        for line in part.open():
            row = json.loads(line)
            if row['reason'] == 'assistant_leading_whitespace_only':
                rows.append(row)
    expected = sum(n for (_, reason), n in source_report['pair_reason_counts']
                   if reason == 'assistant_leading_whitespace_only')
    assert len(rows) == expected
    args.output.mkdir(parents=True, exist_ok=False)
    config = json.loads(args.config.read_text())
    storage = StorageConfig(endpoint_url=config['s3_endpoint'], region=config['s3_region'],
                            credentials_file=config['s3_credentials_file'],
                            cache_dir=str(args.output / 'sources'))
    local = threading.local()

    def read(row):
        if not hasattr(local, 'reader'):
            local.reader = Reader(storage)
        return explain(row, [local.reader.node(row[k]) for k in ('short', 'long')])

    counts, models, prefixes = (collections.Counter() for _ in range(3))
    distinct = collections.defaultdict(set)
    final_only, two_newlines = collections.Counter(), collections.Counter()
    examples = collections.defaultdict(list)
    completed = 0
    with ThreadPoolExecutor(max_workers=6) as pool, (args.output / 'attributed.jsonl').open('x') as out:
        for result in pool.map(read, rows):
            key = result['short']['dataset'], result['category']
            counts[key] += 1
            models[key + (result['model'],)] += 1
            distinct[key].add(result['short']['id'])
            final_only[key] += result['only_final_assistant_differs']
            two_newlines[key] += result['all_changes_exactly_two_newlines']
            for change in result['changed_messages']:
                prefixes[tuple(change['prefixes'])] += 1
            examples[key].append(result)
            examples[key] = sorted(examples[key], key=lambda r: r['short']['id'])[:3]
            out.write(json.dumps(result, ensure_ascii=False) + '\n')
            completed += 1
            if completed % 500 == 0:
                print(json.dumps({'completed': completed, 'expected': expected}), flush=True)
    assert completed == expected
    report = {'passed': True, 'diagnostic_only': True, 'local_only': True, 's3_writes': False,
              'candidate_pairs': completed,
              'categories': [{'dataset': k[0], 'category': k[1], 'pairs': n,
                              'distinct_short_records': len(distinct[k]),
                              'only_final_assistant_pairs': final_only[k],
                              'exact_two_newline_pairs': two_newlines[k]}
                             for k, n in sorted(counts.items())],
              'model_category_counts': [[list(k), n] for k, n in sorted(models.items())],
              'changed_message_whitespace_prefix_counts': [[list(k), n] for k, n in prefixes.most_common()],
              'limitation': 'Only the discovered leading-whitespace candidates are checked. '
              'Canonical visible histories retain the existing ignored fields. '
              'The diagnostic does not authorize merges or establish semantic equivalence.'}
    (args.output / 'report.json').write_text(json.dumps(report, ensure_ascii=False, indent=2) + '\n')
    selected = [r for key in sorted(examples) for r in examples[key]]
    (args.output / 'examples.json').write_text(json.dumps(selected, ensure_ascii=False, indent=2) + '\n')
    (args.output / 'SUCCEEDED').write_text('All selected histories and source hashes verified locally.\n')
    print(json.dumps({'completed': completed, 'categories': report['categories'], 'passed': True}), flush=True)


if __name__ == '__main__':
    main()
