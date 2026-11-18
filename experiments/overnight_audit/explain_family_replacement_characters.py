"""Find full visible prefixes that differ only at Unicode replacement characters in one local family."""

import argparse
import hashlib
import os
import re
from bisect import bisect_left
from collections import Counter, defaultdict
from functools import cache
from pathlib import Path

import audit_core as audit
import orjson

BROKEN = '\ufffd'
RUN = re.compile(BROKEN + '+')
NON_ASCII = re.compile(r'[^\x00-\x7f]')


def characters(text):
    widths, extra = {}, 0
    for run in RUN.finditer(text):
        width = run.end() - run.start()
        widths[run.start() - extra] = width
        extra += width - 1
    return RUN.sub(BROKEN, text), widths


def compare_characters(left, right):
    a, aw = left
    b, bw = right
    if len(a) != len(b):
        return None
    start = 0
    edits = []
    for i in sorted(aw.keys() | bw.keys()):
        if a[start:i] != b[start:i]:
            return None
        start = i + 1
        if i in aw and i in bw:
            if aw[i] != bw[i]:
                return None
        else:
            side, width, observed = ('left', aw[i], b[i]) if i in aw else ('right', bw[i], a[i])
            if width > 4 or observed.isascii():
                return None
            edits.append((side, i, width, observed))
    return edits if a[start:] == b[start:] else None


def cached_members(family):
    for line in (family / 'family.jsonl').read_bytes().splitlines():
        row = orjson.loads(line)
        ref = row['ref']
        data = (family / 'sources' / ref['sha256']).read_bytes()
        assert len(data) == ref['bytes'] and hashlib.sha256(data).hexdigest() == ref['sha256']
        yield row['sub'], row['scope_hash'], orjson.loads(data)


def candidate_pairs(rows):
    rows.sort()
    keys = [r[0] for r in rows]
    for broad, sub, raw in rows:
        for j in range(bisect_left(keys, broad), len(rows)):
            key, other_sub, other_raw = rows[j]
            if key[:len(broad)] != broad:
                break
            if len(key) > len(broad) or sub < other_sub:
                yield sub, raw, other_sub, other_raw


def compare_family(members, write_witness):
    intern, broad_keys, metadata, groups = {}, {}, [], defaultdict(list)
    affected = 0
    for sub, scope, node in members:
        seq = []
        for msg in audit.stable_messages(node)[0]:
            literal = audit.dumps(msg)
            num = intern.get(literal)
            if num is None:
                num = intern[literal] = len(metadata)
                text = msg['content']
                folded, widths = characters(text)
                # The broad key is used solely to enumerate potential prefixes.
                # Every changed field is then checked against its complete literal text.
                key = audit.dumps({**msg, 'content': NON_ASCII.sub(BROKEN, folded)})
                broad = broad_keys.setdefault(key, len(broad_keys))
                metadata.append((broad, msg['role'], (folded, widths)))
            seq.append(num)
        affected += any(metadata[num][2][1] for num in seq)
        groups[scope].append((tuple(metadata[num][0] for num in seq), sub, tuple(seq)))

    @cache
    def compare_message(x, y):
        ax, role, left = metadata[x]
        by, _, right = metadata[y]
        assert ax == by
        result = compare_characters(left, right)
        if not result:
            return None
        return {'role': role, 'edits': result}

    counts, roles, widths = Counter(), Counter(), Counter()
    affected_subs, equal_members, strict_shorts = set(), set(), set()
    for rows in groups.values():
        for sub, raw, other_sub, other_raw in candidate_pairs(rows):
            counts['broad_candidates'] += 1
            changes = []
            for i, (x, y) in enumerate(zip(raw, other_raw)):
                if x != y:
                    result = compare_message(x, y)
                    if result is None:
                        break
                    changes.append({'position': i, **result})
            else:
                if not changes:
                    counts['already_literal_visible_prefix'] += 1
                    continue
                kind = 'equal_length' if len(raw) == len(other_raw) else 'strict_prefix'
                counts[kind] += 1
                affected_subs.update((sub, other_sub))
                if kind == 'strict_prefix':
                    strict_shorts.add(sub)
                else:
                    equal_members.update((sub, other_sub))
                roles.update(set(change['role'] for change in changes))
                for change in changes:
                    widths.update(edit[2] for edit in change['edits'])
                write_witness({'left_sub': sub, 'right_sub': other_sub, 'kind': kind,
                               'lengths': [len(raw), len(other_raw)], 'differing_messages': changes})
    return {'cached_sources_reread': sum(map(len, groups.values())),
              'records_with_replacement_in_visible_content': affected, 'pair_counts': counts,
              'distinct_records_in_verified_pairs': len(affected_subs),
              'distinct_strict_shorter_records': len(strict_shorts), 'distinct_equal_length_members': len(equal_members),
              'pair_counts_by_changed_role_with_overlap': roles,
              'edit_occurrences_by_run_width': {str(k): n for k, n in sorted(widths.items())}}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--family', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    os.umask(0o077)
    assert (args.family / 'SUCCEEDED').is_file()
    parent = orjson.loads((args.family / 'report.json').read_bytes())
    args.output.mkdir(parents=True, exist_ok=False)
    with (args.output / 'witnesses.jsonl').open('wb') as out:
        report = compare_family(cached_members(args.family), lambda row: out.write(orjson.dumps(row) + b'\n'))
    report.update(passed=True, input_plan_sha256=parent['input_plan_sha256'], local_only=True,
                  rule='Runs of 1 to 4 replacement characters may align to one concrete non-ASCII character. '
                       'All other visible text and other canonical message fields must match throughout. '
                       'The test does not reconstruct lost text, and the relation is not transitive.')
    assert report['cached_sources_reread'] == parent['family_count']
    (args.output / 'report.json').write_bytes(orjson.dumps(report, option=orjson.OPT_INDENT_2) + b'\n')
    (args.output / 'SUCCEEDED').write_text('All candidate visible histories compared from complete cached sources.\n')
    print(orjson.dumps(report).decode())


if __name__ == '__main__':
    main()
