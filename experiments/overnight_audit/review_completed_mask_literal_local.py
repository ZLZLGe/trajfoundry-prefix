"""Reproduce the task-placeholder finding from immutable local evidence.

This is a local counterfactual check against the running job's frozen code.
It neither changes production rules nor claims final publication membership.
"""

import argparse
import hashlib
import json
import os
import sys
from pathlib import Path


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--evidence', type=Path, required=True)
    parser.add_argument('--frozen-source', type=Path, required=True)
    parser.add_argument('--tokenplan-evidence', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    os.umask(0o077)
    assert not args.output.exists()
    sys.path.insert(0, str(args.frozen_source))
    import trajectory_prefix.masking as masking
    from trajectory_prefix.mask_evidence import explain_partition
    from trajectory_prefix.mask_job import POLICY
    from trajectory_prefix.mask_verification import raw_relaxed_covers
    from trajectory_prefix.storage import StorageConfig

    assert Path(masking.__file__).resolve().parent.parent == args.frozen_source.resolve()
    candidate = json.loads((args.evidence / 'mask-literal-selected-decision.json').read_text())['candidate']
    proof = json.loads((args.evidence / 'mask-literal-decision-proof.json').read_text())
    nodes = []
    comparisons = []
    for side, prior_side in [('short', 'record'), ('long', 'retained')]:
        ref = candidate[side]
        data = (args.evidence / 'sources' / ref['sha256']).read_bytes()
        assert len(data) == ref['bytes'] and hashlib.sha256(data).hexdigest() == ref['sha256']
        node = json.loads(data)
        nodes.append(node)
        previous = json.loads((args.tokenplan_evidence / (prior_side + '-normalized.json')).read_bytes())
        comparisons.append({'side': side, 'messages_equal_including_reasoning': node['messages'] == previous['messages'],
                            'tools_equal': node['tools'] == previous['tools']})
    chosen = next(row for row in proof['decisions'] if row['record']['id'] == candidate['short']['id'])
    assert chosen['status'] == 'removed' and chosen['retained'] == candidate['long']
    assert any(row['record'] == candidate['long'] and row['status'] == 'kept' for row in proof['decisions'])
    assert any(row['record'] == candidate['short'] and row['retained'] == candidate['long'] and row['passed']
               for row in proof['audit'])
    original = raw_relaxed_covers(*nodes, candidate['short'], candidate['long'], POLICY)
    token = masking.DISABLED_LABELS.set(frozenset({'<MASK>'}))
    try:
        counterfactual = raw_relaxed_covers(*nodes, candidate['short'], candidate['long'], POLICY)
    finally:
        masking.DISABLED_LABELS.reset(token)
    explanation = list(explain_partition([chosen], StorageConfig(cache_dir=str(args.evidence / 'sources'))))[0]
    assert original and not counterfactual and explanation['necessary_labels'] == ['<MASK>']
    assert all(c['messages_equal_including_reasoning'] and c['tools_equal'] for c in comparisons)
    result = {
        'passed': True, 'local_only': True, 'network_access': False, 'production_changed': False,
        'original_frozen_comparison': original, 'local_comparison_without_MASK': counterfactual,
        'necessary_labels': explanation['necessary_labels'],
        'full_message_counts': [len(n['messages']) for n in nodes],
        'user_task_placeholder_counts': [sum(m['content'].count('<MASK>') for m in n['messages']
                                           if m['role'] == 'user') for n in nodes],
        'cross_channel_comparison': comparisons,
        'frozen_code_sha256': {name: hashlib.sha256((args.frozen_source / 'trajectory_prefix' / name).read_bytes()).hexdigest()
                              for name in ('masking.py', 'mask_verification.py', 'mask_evidence.py')},
        'publication_membership_verified': False,
        'limitation': ('Evidence proves the completed merge decision, not final publication. '
                       'The identical TokenPlan message pair is not an independent task counterexample.'),
    }
    args.output.write_text(json.dumps(result, ensure_ascii=False, indent=2) + '\n')
    print(json.dumps(result, ensure_ascii=False))


if __name__ == '__main__':
    main()
