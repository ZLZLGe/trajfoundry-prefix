"""Explain empty-visible-assistant candidates using complete local reads.

The categories describe the first blocking difference. They do not authorize
dropping thought-only messages or establish complete semantic equivalence.
"""
import argparse
import collections
import json
import os
import threading
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import audit_core as audit

from trajectory_prefix.storage import Reader, StorageConfig

EMPTY = {'role': 'assistant', 'content': ''}


def raw_positions(node):
    positions, seen = [], False
    for i, message in enumerate(node['messages']):
        if message['role'] == 'assistant':
            visible = bool(message['content'].rstrip(' \t\r\n') or message.get('tool_calls'))
            if (not visible and seen and set(message) <= {'role', 'content', 'tool_calls'} | audit.REASONING
                    and not any(message.get(k) for k in audit.REASONING)):
                continue
            seen |= visible
        positions.append(i)
    return positions


def explain(row, nodes):
    messages = [audit.stable_messages(node)[0] for node in nodes]
    positions = [raw_positions(node) for node in nodes]
    assert all(len(m) == len(p) for m, p in zip(messages, positions))
    vectors = [list(map(audit.dumps, sequence)) for sequence in messages]
    trimmed = [[audit.dumps(m) for m in sequence if m != EMPTY] for sequence in messages]
    assert audit.is_prefix(*trimmed)
    common = 0
    for a, b in zip(*vectors):
        if a != b:
            break
        common += 1
    assert common < min(map(len, messages))
    raw = [node['messages'][p[common]] for node, p in zip(nodes, positions)]
    at = [m[common] for m in messages]
    observations = []
    for empty_side, combined_side in [(0, 1), (1, 0)]:
        if at[empty_side] != EMPTY:
            continue
        thought = raw[empty_side].get('reasoning_content')
        other = raw[combined_side]
        if isinstance(thought, str) and thought:
            next_index = common + 1
            if (other.get('role') == 'assistant' and other.get('reasoning_content') == thought
                    and next_index < len(vectors[empty_side])
                    and vectors[empty_side][next_index] == vectors[combined_side][common]):
                altered = [list(v) for v in vectors]
                del altered[empty_side][common]
                observations.append({'kind': 'same_thought_split_from_action', 'split_side': empty_side,
                                     'thought_characters': len(thought),
                                     'single_empty_removal_restores_prefix': audit.is_prefix(*altered)})
            content = other.get('content', '')
            if other.get('role') == 'assistant' and len(thought) >= 20 and thought in content:
                offset = content.index(thought)
                observations.append({'kind': 'same_thought_embedded_in_visible_content',
                                     'thought_side': empty_side, 'thought_characters': len(thought),
                                     'content_prefix': audit.safe_excerpt(content[:offset], 120),
                                     'content_suffix': audit.safe_excerpt(content[offset + len(thought):], 120)})
    if observations:
        category = observations[0]['kind']
    elif at[0] == EMPTY and common == len(messages[0]) - 1:
        if any(raw[0].get(k) for k in audit.REASONING):
            category = 'terminal_thought_only_response_differs'
        else:
            category = 'terminal_fully_empty_response_differs'
    else:
        category = 'internal_empty_assistant_layout_differs'
    return {**row, 'category': category, 'observations': observations,
            'first_difference_stable_index': common,
            'first_difference_raw_indexes': [p[common] for p in positions],
            'first_difference_roles': [m['role'] for m in at],
            'first_difference_reasoning_chars': [len(m.get('reasoning_content') or '') for m in raw],
            'full_visible_prefix_after_removing_empty_positions': True,
            'diagnostic_only': True}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--attribution', type=Path, required=True)
    parser.add_argument('--config', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    assert (args.attribution / 'SUCCEEDED').is_file()
    os.umask(0o077)
    args.output.mkdir(parents=True, exist_ok=False)
    config = json.loads(args.config.read_text())
    storage = StorageConfig(endpoint_url=config['s3_endpoint'], region=config['s3_region'],
                            credentials_file=config['s3_credentials_file'], cache_dir=str(args.output / 'sources'))
    rows = []
    for part in sorted((args.attribution / 'attributed').glob('part-*')):
        for line in part.open():
            row = json.loads(line)
            if row['reason'] == 'empty_visible_assistant_position':
                rows.append(row)
    local = threading.local()

    def read(row):
        if not hasattr(local, 'reader'):
            local.reader = Reader(storage)
        return explain(row, [local.reader.node(row[side]) for side in ['short', 'long']])

    counts, observations, examples = collections.Counter(), collections.Counter(), {}
    completed = 0
    with ThreadPoolExecutor(max_workers=6) as pool, (args.output / 'attributed.jsonl').open('x') as stream:
        for row in pool.map(read, rows):
            stream.write(json.dumps(row, ensure_ascii=False) + '\n')
            key = (row['short']['dataset'], row['category'])
            counts[key] += 1
            for observation in row['observations']:
                observations[row['short']['dataset'], observation['kind'],
                             observation.get('single_empty_removal_restores_prefix')] += 1
            examples.setdefault(key, row)
            completed += 1
            if completed % 50 == 0:
                print(json.dumps({'completed': completed, 'expected': len(rows)}), flush=True)
    assert completed == len(rows)
    report = {'passed': True, 'local_only': True, 's3_writes': False, 'candidate_pairs': completed,
              'category_counts': [[list(k), n] for k, n in sorted(counts.items())],
              'observation_counts': [[list(k), n] for k, n in sorted(observations.items())],
              'limitation': 'Categories describe the first blocking difference; matching text does not authorize deleting reasoning-only messages.'}
    (args.output / 'report.json').write_text(json.dumps(report, ensure_ascii=False, indent=2) + '\n')
    (args.output / 'examples.json').write_text(json.dumps(list(examples.values()), ensure_ascii=False, indent=2) + '\n')
    (args.output / 'SUCCEEDED').write_text('All selected empty-assistant candidates reread and explained locally.\n')
    print(json.dumps({'completed': completed, 'passed': True}), flush=True)


if __name__ == '__main__':
    main()
