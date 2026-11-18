"""Locate candidate terminal cwd replays across sessionless filenames in a completed retained-set index."""

import argparse
import hashlib
import os
import re
import struct
from collections import Counter
from pathlib import Path

import orjson
from local_runtime import spark_session

COMMAND = re.compile(r'tool_calls/[0-9]+/function/arguments/command\Z')


def fingerprint(ref):
    return hashlib.sha256(orjson.dumps([ref['dataset'], ref['date'], ref['path'], ref['bytes'], ref['sha256']])).digest()


def candidate_key(row, position, earlier):
    # Earlier message hashes have a fixed width. The current shape preserves the
    # function name and argument structure. All non-command strings must be equal.
    unchanged = sorted((k, b[1].hex()) for k, b in row['string_bounds'][position].items() if not COMMAND.fullmatch(k))
    return hashlib.sha256(earlier + row['shapes'][position] +
                          orjson.dumps([row['model'], position, unchanged])).digest()


def census(partition):
    inventory, counts, shorts = bytearray(), Counter(), []
    for row in partition:
        ref = row['ref']
        inventory.extend(fingerprint(ref))
        counts['records'] += 1
        if not ref['path'].startswith('no_session_id_'):
            continue
        counts['sessionless_filename'] += 1
        counts['production_isolated'] += bool(row['isolation'])
        bounds = row['string_bounds']
        assert len(bounds) == len(row['hashes']) == len(row['shapes']) == row['message_count']
        if not bounds:
            continue
        commands = [b for k, b in bounds[-1].items() if COMMAND.fullmatch(k)]
        if commands:
            counts['ends_with_string_command_argument'] += 1
        if any(b[2].startswith(('cd ', 'cd\t')) for b in commands):
            counts['terminal_cd_prefilter'] += 1
            prior = hashlib.sha256(b''.join(row['hashes'][:-1])).digest()
            key = candidate_key(row, len(bounds) - 1, prior)
            shorts.append({'key': key.hex(), 'ref': ref, 'position': len(bounds) - 1,
                           'terminal_hash': row['hashes'][-1].hex(), 'isolation': row['isolation']})
    yield bytes(inventory), counts, shorts


def continuations(partition, keys):
    for row in partition:
        if not row['ref']['path'].startswith('no_session_id_'):
            continue
        prior = hashlib.sha256()
        for i, (value, bounds) in enumerate(zip(row['hashes'][:-1], row['string_bounds'][:-1])):
            if any(COMMAND.fullmatch(k) for k in bounds):
                key = candidate_key(row, i, prior.digest())
                allowed = keys.get(key)
                if allowed is not None and (len(allowed) > 1 or value not in allowed):
                    yield orjson.dumps({'key': key.hex(), 'position': i, 'ref': row['ref'],
                                        'message_hash': value.hex(), 'isolation': row['isolation']}).decode()
            prior.update(value)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--refined', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--workers', type=int, default=6)
    args = parser.parse_args()
    os.umask(0o077)
    args.output.mkdir(parents=True, exist_ok=False)
    index = args.refined / 'recheck-index'
    assert (index / '_SUCCESS').is_file()
    check = orjson.loads((args.refined / 'ordinary-final-recheck/report.json').read_bytes())
    assert check['check_only'] and check['removed_count'] == 0
    expected, digest = set(), hashlib.sha256()
    with (args.refined / 'final-input.jsonl').open('rb') as stream:
        for line in stream:
            digest.update(line)
            f = fingerprint(orjson.loads(line))
            assert f not in expected
            expected.add(f)
    assert digest.hexdigest() == check['input_plan_sha256'] and len(expected) == check['kept_count']
    counts, keys = Counter(), {}
    spark = spark_session('audit-sessionless-terminal-cwd', args.workers)
    try:
        sc = spark.sparkContext
        rdd = sc.pickleFile(str(index), minPartitions=args.workers)
        chunks = rdd.mapPartitions(census).collect()
        with (args.output / 'possible-shorts.jsonl').open('wb') as out:
            for inventory, part_counts, shorts in chunks:
                assert len(inventory) == part_counts['records'] * 32
                for (f,) in struct.iter_unpack('32s', inventory):
                    expected.remove(f)
                counts.update(part_counts)
                for row in shorts:
                    out.write(orjson.dumps(row) + b'\n')
                    keys.setdefault(bytes.fromhex(row['key']), set()).add(bytes.fromhex(row['terminal_hash']))
        assert not expected and counts['records'] == check['kept_count']
        print(orjson.dumps({'census': counts, 'possible_prefix_signatures': len(keys)}).decode(), flush=True)
        del chunks
        if keys:
            available = sc.broadcast(keys)
            rdd.mapPartitions(lambda rows: continuations(rows, available.value)).saveAsTextFile(str(args.output / 'longs'))
            assert (args.output / 'longs/_SUCCESS').is_file()
        report = {'passed': True, 'master': sc.master, 'plan_sha256': digest.hexdigest(), 'counts': counts,
                  'unique_possible_prefix_signatures': len(keys),
                  'all_reference_digests_compared_with_plan': True,
                  'definition': 'Both filenames begin no_session_id_. Look for strict extensions of records '
                                'ending with a string command beginning cd and space or tab. Same model, all earlier '
                                'ordinary message hashes, shaped current message, and all non-command string digests '
                                'must agree. A final decision requires a fresh comparison of the original objects.'}
        (args.output / 'report.json').write_bytes(orjson.dumps(report, option=orjson.OPT_INDENT_2) + b'\n')
        (args.output / 'SUCCEEDED').write_text('Full retained-set index traversed locally.\n')
    finally:
        spark.stop()


if __name__ == '__main__':
    main()
