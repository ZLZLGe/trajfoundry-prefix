"""Tie every retained normalized file's recorded ID to a live-hash-verified published lineage."""

import argparse
import hashlib
import os
from pathlib import Path

import orjson


def verify(root, membership):
    assert membership['passed'] and membership['channel'] == root.name
    published = root / 'published'
    pub = orjson.loads((published / 'report.json').read_bytes())
    stage = orjson.loads((published / 'stages/source-inspection.json').read_bytes())
    location = Path(stage['path'])
    assert stage['publication_id'] == pub['publication_id'] and (location / '_SUCCESS').is_file()
    plan = {}
    digest = hashlib.sha256()
    with (root / 'refined/final-input.jsonl').open('rb') as lines:
        for line in lines:
            digest.update(line)
            ref = orjson.loads(line)
            assert ref['path'] not in plan
            plan[ref['path']] = (ref['bytes'], ref['sha256'])
    assert digest.hexdigest() == membership['ordinary_rechecked_plan_sha256']
    expected = set()
    source_rows = 0
    for part in sorted(location.glob('part-*')):
        with part.open('rb') as lines:
            for line in lines:
                row = orjson.loads(line)
                source_rows += 1
                if row['status'] == 'kept':
                    path = row['path']
                    assert row['final_path'] == path and plan.pop(path) == (row['bytes'], row['sha256'])
                    identifier = row['trajectory_id']
                    assert identifier not in expected
                    expected.add(identifier)
                else:
                    assert row['status'] == 'removed'
    assert not plan and source_rows == stage['rows'] == membership['counts']['A']
    count = len(expected)
    assert count == membership['counts']['C']
    digest = hashlib.sha256()
    with (published / 'lineage.jsonl').open('rb') as lines:
        for line in lines:
            digest.update(line)
            expected.remove(orjson.loads(line)['trajectory_id'])
    assert not expected and digest.hexdigest() == membership['lineage']['sha256']
    return {'passed': True, 'channel': root.name, 'publication_id': stage['publication_id'],
            'live_manifest_sha256': membership['target_manifest_sha256'],
            'live_lineage_sha256': digest.hexdigest(), 'source_inspection_rows': source_rows,
            'distinct_retained_files_with_unique_lineage_id': count}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--channel-root', type=Path, required=True)
    parser.add_argument('--membership', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    os.umask(0o077)
    report = verify(args.channel_root, orjson.loads(args.membership.read_bytes()))
    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open('xb') as stream:
        stream.write(orjson.dumps(report, option=orjson.OPT_INDENT_2) + b'\n')
    print(orjson.dumps(report).decode())


if __name__ == '__main__':
    main()
