"""Explain independently found candidates against the unchanged production policy.

This is diagnostic code. It never authorizes removal and never changes a view
used by the production jobs or the independent candidate search.
"""
import copy
import json

import audit_core as audit

from trajectory_prefix.projection import Policy, covers, project
from trajectory_prefix.verification import raw_covers


def combine(a, b):
    assert a['short'] == b['short'] and a['long'] == b['long']
    return {**a, 'modes': sorted(set(a['modes']) | set(b['modes']))}


def only_assistant_leading_space(messages):
    result = copy.deepcopy(messages)
    for message in result:
        if message['role'] == 'assistant':
            message['content'] = message['content'].lstrip(' \t\r\n')
    return result


def empty_counts(node):
    """Use the same visible-content trimming as the stable comparison view."""
    selected = [m for m in node['messages'] if m['role'] == 'assistant'
                and not m['content'].rstrip(' \t\r\n') and not m.get('tool_calls')]
    return {'reasoning_only': sum(any(m.get(k) for k in audit.REASONING) for m in selected),
            'fully_empty': sum(not any(m.get(k) for k in audit.REASONING) for m in selected)}


def explain(row, a, b):
    av, _ = audit.stable_messages(a)
    bv, _ = audit.stable_messages(b)
    for mode in row['modes']:
        assert audit.is_prefix(audit.view(av, mode), audit.view(bv, mode))
    policy = Policy(extra_information='messages_only')
    ap = project(a, row['short']['dataset'], row['short']['date'], policy)
    bp = project(b, row['long']['dataset'], row['long']['date'], policy)
    projected = covers(ap, bp)
    raw = raw_covers(a, b, row['short'], row['long'], policy)
    same_scope = ap.scope == bp.scope
    isolation = sorted({p.isolation_reason for p in (ap, bp) if not p.mergeable})
    common = 0
    for x, y in zip(ap.tokens, bp.tokens):
        if x != y:
            break
        common += 1
    pm = [[json.loads(t) for t in p.tokens] for p in (ap, bp)]
    # Canonical bytes avoid treating True, 1 and 1.0 as equal JSON values.
    lead = [list(map(audit.dumps, only_assistant_leading_space(m))) for m in pm]
    leading = bool(lead[0]) and len(lead[0]) <= len(lead[1]) and lead[0] == lead[1][:len(lead[0])]
    if projected != raw:
        reason = 'production_verifier_disagreement'
    elif projected:
        reason = 'covered_by_unchanged_production_rule'
    elif not same_scope:
        reason = 'identity_boundary'
    elif isolation:
        reason = 'protected:' + '+'.join(isolation)
    elif leading:
        reason = 'assistant_leading_whitespace_only'
    elif 'text_format' in row['modes']:
        reason = 'text_or_argument_format'
    elif 'without_headers' in row['modes']:
        reason = 'leading_system_or_developer_difference'
    elif 'without_notices' in row['modes']:
        reason = 'runtime_notice_difference'
    elif 'without_empty_visible_assistant' in row['modes']:
        reason = 'empty_visible_assistant_position'
    elif 'body_format' in row['modes']:
        reason = 'combined_header_notice_format'
    elif 'marker_labels' in row['modes']:
        reason = 'marker_type_difference'
    else:
        reason = 'independent_projection_difference'
    identity_names = ['dataset', 'model', 'harness', 'user', 'end_user', 'kind', 'session_or_date']
    result = {
        **row, 'diagnostic_only': True, 'full_objects_reverified': True,
        'reason': reason, 'model': a.get('model', ''), 'harness': a.get('harness', ''),
        'same_scope': same_scope, 'isolation_reasons': isolation,
        'identity_differences': [k for k, x, y in zip(identity_names, json.loads(ap.scope), json.loads(bp.scope)) if x != y],
        'production_covers': projected, 'production_raw_covers': raw,
        'eligible_same_scope': same_scope and not isolation,
        'message_counts': [len(ap.tokens), len(bp.tokens)],
        'empty_assistant_counts_after_visible_trimming': [empty_counts(a), empty_counts(b)],
        'common_production_messages': common,
        'only_assistant_leading_whitespace_would_cover': leading,
        'first_difference': audit.difference(pm[0][common], pm[1][common])
            if common < min(len(pm[0]), len(pm[1])) else None,
    }
    return result


def sample_rank(row):
    return audit.digest(audit.dumps([row['short']['id'], row['long']['id']])).hex()


def bounded(a, b):
    values = {sample_rank(r): r for r in a + b}
    return [values[k] for k in sorted(values)[:5]]
