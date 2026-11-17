"""Freeze refs to the actual published objects using completed publication receipts."""
import argparse
import hashlib
import json
import os
from pathlib import Path

from trajectory_prefix.storage import Reader, StorageConfig, file_sha256, reference


def main():
    p = argparse.ArgumentParser()
    p.add_argument('--publication-run', type=Path, required=True)
    p.add_argument('--config', type=Path, required=True)
    p.add_argument('--output', type=Path, required=True)
    p.add_argument('--channels', nargs='+', required=True)
    args = p.parse_args()
    os.umask(0o077)
    args.output.mkdir(parents=True, exist_ok=False)
    config = json.loads(args.config.read_text())
    reader = Reader(StorageConfig(endpoint_url=config['s3_endpoint'], region=config['s3_region'],
                                  credentials_file=config['s3_credentials_file']))
    total, counts, manifests = 0, {}, []
    digest = hashlib.sha256()
    with (args.output / 'input-plan.jsonl').open('xb') as stream:
        for channel in args.channels:
            root = args.publication_run / channel
            assert (root / 'published/SUCCEEDED').is_file()
            assert (root / 'refined/SUCCEEDED').is_file()
            report = json.loads((root / 'published/report.json').read_text())
            assert report['passed'] and report['publish_selection'] is False
            local = root / 'published/normalized-manifest.json'
            manifest_ref = reference({'dataset': channel, 'date': '2026-10-30', 'path': 'manifest.json',
                                      'uri': report['normalized_uri'] + 'manifest.json',
                                      'sha256': file_sha256(local), 'bytes': local.stat().st_size})
            reader.read(manifest_ref)
            manifests.append(manifest_ref)
            count = 0
            with (root / 'refined/final-input.jsonl').open() as inputs:
                for line in inputs:
                    ref = reference(json.loads(line))
                    ref.pop('local_path', None)
                    ref['uri'] = report['normalized_uri'] + ref['path']
                    data = (json.dumps(ref, sort_keys=True, ensure_ascii=False) + '\n').encode()
                    stream.write(data)
                    digest.update(data)
                    count += 1
            assert count == report['kept_count']
            counts[channel] = count
            total += count
    receipt = {'input_count': total, 'counts': counts, 'published_manifests': manifests,
               'input_plan_sha256': digest.hexdigest(), 'input_versions_verified': True}
    (args.output / 'receipt.json').write_text(json.dumps(receipt, ensure_ascii=False, indent=2) + '\n')
    print(json.dumps({'input_count': total, 'counts': counts, 'plan': str(args.output / 'input-plan.jsonl')}, ensure_ascii=False))


if __name__ == '__main__':
    main()
