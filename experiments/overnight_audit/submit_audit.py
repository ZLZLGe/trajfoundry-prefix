"""Freeze independent audit code and submit it to the user's existing queue."""
import argparse
import hashlib
import json
import os
import shutil
import subprocess
import sys
import time
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO / 'scripts'))
import submit_cluster as submit  # noqa: E402


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--config', type=Path, required=True)
    parser.add_argument('--input-plan', type=Path, required=True)
    parser.add_argument('--expected-count', type=int, required=True)
    parser.add_argument('--control', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--executors', type=int, default=4)
    parser.add_argument('--partitions', type=int, default=512)
    args = parser.parse_args()
    config = json.loads(args.config.read_text())
    assert config['queue'] == 'root.datacenter.pjcc.agent_trajectory'
    control, output = (submit.shared_path(p, config['shared_root']) for p in (args.control, args.output))
    assert not control.exists() and not output.exists()
    control.mkdir(parents=True, mode=0o700)
    (control / 'eventLog').mkdir(mode=0o700)
    for folder in ['src', 'scripts', 'experiments']:
        shutil.copytree(REPO / folder, control / 'app' / folder,
                        ignore=shutil.ignore_patterns('__pycache__', '.pytest_cache'))
    pod, command = submit.command(config, control, output, input_plan=args.input_plan,
                                  partitions=args.partitions, executors=args.executors,
                                  executor_cores=8, input_cache=False)
    app = command.index('local://' + str(control / 'app/scripts/run.py'))
    command[app:] = ['local://' + str(control / 'app/experiments/overnight_audit/run_audit.py'),
                    '--input-plan', str(args.input_plan), '--expected-count', str(args.expected_count),
                    '--output', str(output), '--partitions', str(args.partitions),
                    '--s3-endpoint', config['s3_endpoint'], '--s3-region', config['s3_region'],
                    '--s3-credentials-file', config['s3_credentials_file']]
    for role in ['driver', 'executor']:
        template = submit.pod_template(config, control, output, driver=role == 'driver', pod=pod)
        (control / (role + '-pod.json')).write_text(json.dumps(template, indent=2) + '\n')
    (control / 'empty.properties').write_text('# Deliberately isolated Spark defaults.\n')
    hashes = {str(p.relative_to(control / 'app')): hashlib.sha256(p.read_bytes()).hexdigest()
              for p in (control / 'app').rglob('*') if p.is_file()}
    (control / 'source-hashes.json').write_text(json.dumps(hashes, indent=2) + '\n')
    plan = {'driver_pod': pod, 'queue': config['queue'], 'output': str(output),
            'command': command, 'input_plan': str(args.input_plan), 'expected_count': args.expected_count,
            'repository_commit': subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=REPO, text=True).strip()}
    (control / 'plan.json').write_text(json.dumps(plan, indent=2) + '\n')
    print(json.dumps({'event': 'prepared', 'driver_pod': pod, 'queue': config['queue']}), flush=True)
    env = dict(os.environ, JAVA_HOME=config['submit_java_home'])
    for key in ['HTTP_PROXY', 'HTTPS_PROXY', 'ALL_PROXY', 'http_proxy', 'https_proxy', 'all_proxy',
                'PYSPARK_DRIVER_PYTHON', 'PYSPARK_PYTHON']:
        env.pop(key, None)
    with (control / 'submit.log').open('w') as log:
        result = subprocess.run(command, env=env, stdout=log, stderr=subprocess.STDOUT)
    state = submit.fetch_pod(config, pod)
    # Driver success can precede shared-filesystem visibility of its final files.
    for _ in range(20):
        if (output / 'SUCCEEDED').is_file() and (output / 'report.json').is_file():
            break
        if state.get('status', {}).get('phase') != 'Succeeded':
            break
        time.sleep(2)
    report = json.loads((output / 'report.json').read_text()) if (output / 'report.json').exists() else {}
    phase = state.get('status', {}).get('phase')
    passed = (result.returncode == 0 and phase == 'Succeeded' and report.get('passed') is True
              and report.get('input_count') == args.expected_count
              and state['metadata']['labels']['queue'] == config['queue'])
    final = {'passed': passed, 'driver_pod': pod, 'driver_uid': state['metadata']['uid'],
             'driver_phase': phase, 'queue': config['queue'], 'submit_exit_code': result.returncode}
    (control / 'final.json').write_text(json.dumps(final, indent=2) + '\n')
    print(json.dumps(final), flush=True)
    raise SystemExit(0 if passed else 2)


if __name__ == '__main__':
    main()
