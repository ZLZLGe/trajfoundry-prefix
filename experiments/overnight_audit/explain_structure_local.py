"""Read and explain the verified mid-history candidates on this host only."""
import argparse
import collections
import json
import os
import re
import threading
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import attribution_core
import audit_core as audit

from trajectory_prefix.storage import Reader, StorageConfig


def kind(message):
    text = message['content'].strip()
    if text == 'The Bash tool shell is Git Bash.':
        return 'shell_notice'
    if text.startswith('<runtime_context>') and text.endswith('</runtime_context>'):
        return 'runtime_context'
    if re.fullmatch(r'<total_tokens>[0-9]+ tokens left</total_tokens>', text):
        return 'token_budget'
    if text.startswith('<total_tokens>') and 'First privately list what you need next;' in text:
        return 'token_budget_and_planning_hint'
    if text.startswith('Available agent types for the Agent tool:'):
        return 'agent_catalog'
    if text.startswith('Called the Read tool with the following input:'):
        return 'cached_file_evidence'
    if text.startswith("The task tools haven't been used recently."):
        return 'task_tracking_reminder'
    return 'other_system_or_developer_content'


def slots(messages):
    position = 0
    result = collections.defaultdict(list)
    for message in messages:
        if message['role'] in {'system', 'developer'}:
            result[position].append(message)
        else:
            position += 1
    return result, position


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--config', type=Path, required=True)
    parser.add_argument('--probe', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    os.umask(0o077)
    assert (args.probe / 'SUCCEEDED').is_file()
    args.output.mkdir(parents=True, exist_ok=False)
    config = json.loads(args.config.read_text())
    storage = StorageConfig(endpoint_url=config['s3_endpoint'], region=config['s3_region'],
                            credentials_file=config['s3_credentials_file'], cache_dir=str(args.output / 'sources'))
    candidates = []
    for file in sorted((args.probe / 'verified').glob('part-*')):
        for line in file.open():
            row = json.loads(line)
            if not row['literal_coverage'] and not any(row['full_view_coverage']['without_headers']):
                candidates.append(row)
    local = threading.local()

    def explain(row):
        if not hasattr(local, 'reader'):
            local.reader = Reader(storage)
        a, b = (local.reader.node(row[k]) for k in ('short', 'long'))
        pair = {k: row[k] for k in ('short', 'long')} | {'modes': ['without_system_developer']}
        result = attribution_core.explain(pair, a, b)
        (sa, na), (sb, nb) = [slots(audit.stable_messages(n)[0]) for n in (a, b)]
        assert na <= nb
        changed = []
        for position in sorted(sa.keys() | sb.keys()):
            # Ignore new system messages after the covered part of the history.
            if position > na or (position == na and not sa.get(position)):
                continue
            left, right = sa.get(position, []), sb.get(position, [])
            if list(map(audit.dumps, left)) == list(map(audit.dumps, right)):
                continue
            changed.append({'body_position': position,
                            'left_kinds': [kind(m) for m in left],
                            'right_kinds': [kind(m) for m in right],
                            'left_chars': sum(len(m['content']) for m in left),
                            'right_chars': sum(len(m['content']) for m in right)})
        result['changed_system_slots'] = changed
        result['system_content_kinds'] = sorted({k for slot in changed
            for key in ['left_kinds', 'right_kinds'] for k in slot[key]})
        return result

    counts, categories = collections.Counter(), collections.Counter()
    total = 0
    examples = {}
    with (args.output / 'attributed.jsonl').open('x') as stream, ThreadPoolExecutor(max_workers=6) as pool:
        for row in pool.map(explain, candidates):
            stream.write(json.dumps(row, ensure_ascii=False) + '\n')
            counts[row['short']['dataset'], row['reason']] += 1
            key = (row['short']['dataset'], row['reason'], tuple(row['system_content_kinds']))
            categories[key] += 1
            examples.setdefault(key, row)
            total += 1
            if total % 100 == 0:
                stream.flush()
                print(json.dumps({'completed': total, 'expected': len(candidates)}), flush=True)
    assert total == len(candidates)
    report = {'passed': True, 'local_only': True, 's3_writes': False, 'diagnostic_only': True,
              'candidate_pairs': total,
              'reason_counts': [[list(k), v] for k, v in sorted(counts.items())],
              'system_kind_sets': [[list(k), v] for k, v in sorted(categories.items())],
              'limitation': 'System-message labels are descriptive, not a rule allowing their removal.'}
    (args.output / 'report.json').write_text(json.dumps(report, ensure_ascii=False, indent=2) + '\n')
    (args.output / 'examples.json').write_text(json.dumps(list(examples.values()), ensure_ascii=False, indent=2) + '\n')
    (args.output / 'SUCCEEDED').write_text('Local full-object structure attribution completed.\n')
    print(json.dumps({'completed': total, 'passed': True}), flush=True)


if __name__ == '__main__':
    main()
