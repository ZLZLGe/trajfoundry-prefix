"""Locate terminal progress-description requests and their inherited histories in one local family."""

import argparse
import hashlib
import os
from bisect import bisect_left
from collections import Counter, defaultdict
from pathlib import Path

import audit_core as audit
import orjson

PROMPT = 'Describe your most recent action in 3-5 words using present tense (-ing).'
FORK = '<fork-boilerplate>'


def classify(record, intern, source_dir):
    ref = record['ref']
    data = (source_dir / ref['sha256']).read_bytes()
    if len(data) != ref['bytes'] or hashlib.sha256(data).hexdigest() != ref['sha256']:
        raise ValueError('Cached source changed')
    source = orjson.loads(data)
    messages, _ = audit.stable_messages(source)
    full = tuple(intern.setdefault(audit.dumps(m), len(intern)) for m in messages)
    raw = source['messages']
    idx = max(i for i, m in enumerate(raw) if m['role'] == 'user')
    text = raw[idx]['content']
    tail = raw[idx + 1:]
    terminal = (text.startswith(PROMPT) and tail and tail[-1]['role'] == 'assistant'
                and all(m['role'] in {'system', 'developer'} for m in tail[:-1]))
    stable_user = max(i for i, m in enumerate(messages) if m['role'] == 'user')
    assert messages[stable_user]['content'] == text
    result_type = None
    if terminal:
        last = tail[-1]
        if last.get('tool_calls'):
            result_type = 'assistant_with_tool_calls'
        elif last['content'].strip():
            result_type = 'visible_answer_without_tool_calls'
        else:
            result_type = 'empty_visible_answer_without_tool_calls'
    # The occurrence in the *next* user item determines whether another record
    # merely issues the same kind of helper request after this inherited prefix.
    next_kind = []
    for m in messages:
        if m['role'] in {'system', 'developer'}:
            next_kind.append('instruction')
        elif m['role'] == 'user' and PROMPT in m['content']:
            next_kind.append('progress_prompt_present')
        else:
            next_kind.append(m['role'])
    return {'sub': record['sub'], 'scope': record['scope_hash'], 'history': full,
            'next_kind': next_kind, 'cut_index': stable_user, 'terminal': bool(terminal),
            'last_user_contains_prompt': PROMPT in text, 'last_user_starts_prompt': text.startswith(PROMPT),
            'history_contains_worker_fork': any(m['role'] == 'user' and m['content'].startswith(FORK)
                                                for m in raw),
            'result_type': result_type}


def witnesses(row, ordered, histories):
    inherited = row['history'][:row['cut_index']]
    if not inherited:
        return {}
    start = bisect_left(histories, inherited)
    result = {}
    for i in range(start, len(ordered)):
        other = ordered[i]
        sequence = other['history']
        if sequence[:len(inherited)] != inherited:
            break
        if row['sub'] == other['sub'] or len(sequence) == len(inherited):
            continue
        first = next((kind for kind in other['next_kind'][len(inherited):] if kind != 'instruction'), None)
        if first is None or first == 'progress_prompt_present':
            category = 'only_another_progress_prompt_or_instructions'
        elif other['last_user_contains_prompt']:
            category = 'different_next_step_in_record_ending_with_prompt_text'
        else:
            category = 'different_next_step_and_terminal_user_without_prompt_text'
        result.setdefault(category, other['sub'])
    return result


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--family', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    os.umask(0o077)
    assert (args.family / 'SUCCEEDED').is_file()
    parent = orjson.loads((args.family / 'report.json').read_bytes())
    args.output.mkdir(parents=True, exist_ok=False)
    intern, groups, census, results = {}, defaultdict(list), Counter(), Counter()
    with (args.family / 'family.jsonl').open('rb') as stream:
        for line in stream:
            row = classify(orjson.loads(line), intern, args.family / 'sources')
            groups[row['scope']].append(row)
            census['records'] += 1
            census['history_contains_worker_fork'] += row['history_contains_worker_fork']
            census['last_user_contains_prompt'] += row['last_user_contains_prompt']
            census['last_user_starts_prompt'] += row['last_user_starts_prompt']
            if row['terminal']:
                results[row['result_type']] += 1
    assert census['records'] == parent['family_count']
    categories = ['different_next_step_and_terminal_user_without_prompt_text',
                  'different_next_step_in_record_ending_with_prompt_text',
                  'only_another_progress_prompt_or_instructions']
    coverage, outcome_coverage = Counter(), Counter()
    with (args.output / 'progress-histories.jsonl').open('wb') as output:
        for rows in groups.values():
            ordered = sorted(rows, key=lambda r: (r['history'], r['sub']))
            histories = [r['history'] for r in ordered]
            for row in ordered:
                if not row['terminal']:
                    continue
                found = witnesses(row, ordered, histories)
                best = next((name for name in categories if name in found), 'no_other_extension')
                coverage[best] += 1
                outcome_coverage[row['result_type'] + '/' + best] += 1
                output.write(orjson.dumps({'sub': row['sub'], 'earlier_visible_messages': row['cut_index'],
                                           'terminal_assistant': row['result_type'], 'class': best,
                                           'witnesses_by_class': found}) + b'\n')
    assert sum(coverage.values()) == sum(results.values())
    report = {'passed': True, 'local_only': True, 'input_plan_sha256': parent['input_plan_sha256'],
              'family': parent['family'], 'counts': census, 'terminal_assistant': results,
              'history_coverage_by_priority': coverage, 'coverage_by_assistant': outcome_coverage,
              'distinct_canonical_visible_messages': len(intern),
              'interpretation': 'Each witness extends every visible message strictly before the terminal request. '
                                'The comparison preserves canonical JSON equality, assigns one class per record, '
                                'and does not compare or remove the suffix with the request and answer.'}
    (args.output / 'report.json').write_bytes(orjson.dumps(report, option=orjson.OPT_INDENT_2) + b'\n')
    (args.output / 'SUCCEEDED').write_text('Complete cached objects checked and inherited histories compared.\n')
    print(orjson.dumps({k: v for k, v in report.items() if k not in {'family', 'interpretation'}}).decode())


if __name__ == '__main__':
    main()
