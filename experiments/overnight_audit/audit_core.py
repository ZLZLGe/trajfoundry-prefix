"""Independent diagnostic views. These experiments never authorize deletions."""
from __future__ import annotations

import copy
import hashlib
import json
import re
from collections import Counter

MODES = ('stable_messages', 'text_format', 'without_headers', 'without_notices',
         'body_format', 'marker_labels', 'cross_identity')
REASONING = {'reasoning', 'reasoning_content', 'reasoning_details'}
NOTICES = re.compile(r'<(system-reminder|environment_context|local-command-caveat)\b[^>]*>.*?</\1>', re.S)
MASK = re.compile(r'<(?:PERSON|LOCATION|ID_NUMBER|EMAIL|PHONE|PHONE_NUMBER|ADDRESS|IP_ADDRESS|ORGANIZATION|SENSITIVE|MASKED|REDACTED)>|\[(?:MASKED|REDACTED)\]', re.I)
SECRET = re.compile(r'\b(?:sk-[A-Za-z0-9_-]{12,}|gh[pousr]_[A-Za-z0-9]+|github_pat_[A-Za-z0-9_]+)\b|(?i:Bearer\s+)[A-Za-z0-9._~+/-]{12,}')


def dumps(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(',', ':')).encode()


def digest(value):
    return hashlib.sha256(value).digest()


def ident(value):
    if not isinstance(value, str) or value.strip().lower() in {'', 'none', 'null', 'unknown', 'no_user_id', 'no_session_id'}:
        return ''
    if value.startswith('<') and value.endswith('>'):
        return ''
    return value


def scope(node, ref):
    md = node.get('metadata') or {}
    session = ident(md.get('session_id'))
    return [ref['dataset'], node.get('model', ''), node.get('harness', ''),
            ident(md.get('user_id')), ident(md.get('end_user_id')),
            'session' if session else 'sessionless', session or ref['date']]


def stable_messages(node):
    """Independent reconstruction of visible message order and tool bindings."""
    result, calls, issues = [], {}, []
    next_call, seen_assistant = 0, False
    for raw in node['messages']:
        msg = copy.deepcopy({k: v for k, v in raw.items() if k not in REASONING})
        role, text = msg['role'], msg['content']
        if role in {'system', 'developer'}:
            # Only the documented leading billing header / Claude preamble form.
            lines = text.splitlines(keepends=True)
            allowed = {0}
            if text.startswith("You are Claude Code, Anthropic's official CLI for Claude.\n\n"):
                allowed.add(2)
            for i in allowed:
                if i < len(lines) and lines[i].startswith('x-anthropic-billing-header:'):
                    lines[i] = re.sub(r'\bcch=[0-9a-fA-F]+;', 'cch=<volatile-cch>;', lines[i], count=1)
            msg['content'] = ''.join(lines)
        if role == 'assistant':
            msg['content'] = text.rstrip(' \t\r\n')
            if not msg.get('tool_calls'):
                msg.pop('tool_calls', None)
            visible = bool(msg['content'] or msg.get('tool_calls'))
            if (not visible and seen_assistant and set(raw) <= {'role', 'content', 'tool_calls'} | REASONING
                    and not any(raw.get(k) for k in REASONING)):
                continue
            seen_assistant |= visible
            for call in msg.get('tool_calls', []):
                old = call.get('id')
                if not old or old in calls:
                    issues.append('missing_or_reused_call_id')
                    call['id'] = ['ambiguous', old, next_call]
                else:
                    calls[old] = (next_call, call.get('function', {}).get('name', ''))
                    call['id'] = ['ordinal', next_call]
                next_call += 1
        elif role == 'tool':
            old = msg.get('tool_call_id')
            if old not in calls:
                issues.append('unresolved_tool_result')
            else:
                position, name = calls[old]
                if msg.get('name', '') != name:
                    issues.append('tool_name_mismatch')
                msg['tool_call_id'] = ['ordinal', position]
        result.append(msg)
    return result, sorted(set(issues))


def normalize_value(value):
    if isinstance(value, str):
        return '\n'.join(line.rstrip(' \t') for line in value.replace('\r\n', '\n').replace('\r', '\n').split('\n')).strip()
    if isinstance(value, dict):
        return {k: normalize_value(v) for k, v in value.items()}
    if isinstance(value, list):
        return [normalize_value(v) for v in value]
    return value


def view(messages, mode):
    if mode in {'stable_messages', 'cross_identity'}:
        return messages
    result = copy.deepcopy(messages)
    if mode in {'without_headers', 'body_format'}:
        while result and result[0]['role'] in {'system', 'developer'}:
            result.pop(0)
    if mode in {'without_notices', 'body_format'}:
        for msg in result:
            if msg['role'] in {'system', 'developer', 'user'}:
                msg['content'] = NOTICES.sub('', msg['content'])
        result = [m for m in result if not (m['role'] in {'system', 'developer', 'user'}
                                           and not m['content'].strip() and set(m) <= {'role', 'content'})]
    if mode in {'text_format', 'body_format'}:
        for msg in result:
            msg['content'] = normalize_value(msg['content'])
            for call in msg.get('tool_calls') or []:
                function = call.get('function') or {}
                value = function.get('arguments')
                if isinstance(value, str):
                    try:
                        decoded = json.loads(value)
                        if isinstance(decoded, (dict, list)):
                            value = decoded
                    except ValueError:
                        pass
                if 'arguments' in function:
                    function['arguments'] = normalize_value(value)
    if mode == 'marker_labels':
        def replace(value):
            if isinstance(value, str):
                return MASK.sub('<DIAGNOSTIC_MASK>', value)
            if isinstance(value, list):
                return [replace(v) for v in value]
            if isinstance(value, dict):
                return {k: replace(v) for k, v in value.items()}
            return value
        for msg in result:
            msg['content'] = replace(msg['content'])
            for call in msg.get('tool_calls') or []:
                function = call.get('function') or {}
                if 'arguments' in function:
                    function['arguments'] = replace(function['arguments'])
    return result


def index_node(node, ref):
    messages, bindings = stable_messages(node)
    group = scope(node, ref)
    vector_pool, variants = {}, {}
    chars = sum(len(m['content']) for m in messages if m['role'] in {'user', 'assistant', 'tool'})
    has_tool = any(m['role'] == 'tool' for m in messages)
    for mode in MODES:
        selected = view(messages, mode)
        if len(selected) < 2:
            continue
        if mode == 'cross_identity' and not (len(selected) >= 6 and chars >= 1024 and has_tool):
            continue
        vector = b''.join(digest(dumps(m)) for m in selected)
        variants[mode] = vector_pool.setdefault(vector, vector)
    match = re.search(r'_sub_(\d+)\.jsonl$', ref['path'])
    return {
        'ref': ref, 'scope': digest(dumps(group)), 'scope_values': group,
        'cross_scope': digest(dumps(group[:3] + [ref['date']])),
        'model': node.get('model', ''), 'harness': node.get('harness', ''),
        'sessionless': group[-2] == 'sessionless', 'message_count': len(messages),
        'visible_chars': chars, 'has_tool': has_tool, 'binding_issues': bindings,
        'roles': ''.join({'system': 'S', 'developer': 'D', 'user': 'U', 'assistant': 'A', 'tool': 'T'}[m['role']] for m in messages),
        'family': ref['path'][:match.start()] if match else ref['path'],
        'sub_number': int(match.group(1)) if match else -1,
        'variants': variants,
    }


def sort_entries(row):
    for mode, vector in row['variants'].items():
        group = row['cross_scope'] if mode == 'cross_identity' else row['scope']
        value = {k: row[k] for k in ('ref', 'scope', 'model', 'harness', 'sessionless',
                                    'roles', 'sub_number', 'family', 'binding_issues', 'visible_chars')}
        yield (mode, group, vector, -row['ref']['bytes'], row['ref']['id']), value


def partition_key(key):
    # At least two messages per indexed view; every possible prefix relation
    # shares the first message. The whole chain therefore reaches one sorter.
    return int.from_bytes(digest(key[0].encode() + key[1] + key[2][:32])[:8], 'big')


def common_prefix(a, b):
    i = 0
    while i < min(len(a), len(b)) and a[i:i + 32] == b[i:i + 32]:
        i += 32
    return i // 32


def neighbors(rows):
    previous = None
    for key, row in rows:
        if previous is not None and key[:2] == previous[0][:2]:
            pk, prior = previous
            a, b = pk[2], key[2]
            common = common_prefix(a, b)
            if a == b:
                yield {'kind': 'coverage', 'relation': 'equal', 'mode': key[0],
                       'short': row['ref'], 'long': prior['ref'],
                       'same_identity': row['scope'] == prior['scope']}
                continue
            if len(a) == common * 32:
                yield {'kind': 'coverage', 'relation': 'prefix', 'mode': key[0],
                       'short': prior['ref'], 'long': row['ref'],
                       'same_identity': row['scope'] == prior['scope']}
            elif key[0] == 'stable_messages' and common >= 2:
                if common < len(prior['roles']) and common < len(row['roles']):
                    role_pair = prior['roles'][common] + row['roles'][common]
                    yield {'kind': 'branch_neighbor', 'mode': key[0], 'common_messages': common,
                           'first_differing_roles': role_pair, 'short': prior['ref'], 'long': row['ref'],
                           'same_identity': True, 'sessionless': row['sessionless'],
                           'harness': row['harness'], 'model': row['model'],
                           'max_sub_number': max(prior['sub_number'], row['sub_number']),
                           'same_filename_family': row['family'] == prior['family']}
        previous = key, row


def is_prefix(a, b):
    return bool(a) and len(a) <= len(b) and all(x == y for x, y in zip(a, b))


def safe_excerpt(value, size=1800):
    text = json.dumps(value, ensure_ascii=False) if not isinstance(value, str) else value
    text = SECRET.sub('[CREDENTIAL_REDACTED]', text)
    return text[:size] + ('…' if len(text) > size else '')


def difference(a, b):
    if a == b:
        return None
    if isinstance(a, dict) and isinstance(b, dict):
        changed = []
        for key in sorted(a.keys() | b.keys()):
            if a.get(key) != b.get(key) or (key in a) != (key in b):
                changed.append({'field': key, 'left': safe_excerpt(a.get(key)), 'right': safe_excerpt(b.get(key))})
        return changed
    return [{'left': safe_excerpt(a), 'right': safe_excerpt(b)}]


def verify(row, short_node, long_node):
    a, ai = stable_messages(short_node)
    b, bi = stable_messages(long_node)
    ga, gb = scope(short_node, row['short']), scope(long_node, row['long'])
    assert (ga == gb) == row['same_identity']
    if row['mode'] != 'cross_identity':
        assert ga == gb
    else:
        assert ga[:3] == gb[:3] and row['short']['date'] == row['long']['date']
    av, bv = view(a, row['mode']), view(b, row['mode'])
    if row['kind'] == 'coverage':
        assert is_prefix(av, bv), 'Hash candidate failed complete independent JSON comparison'
    common = 0
    for x, y in zip(a, b):
        if x != y:
            break
        common += 1
    if row['kind'] == 'branch_neighbor':
        assert common == row['common_messages'] and common < min(len(a), len(b))
    names = ['dataset', 'model', 'harness', 'user', 'end_user', 'identity_kind', 'session_or_date']
    result = {**row, 'full_raw_objects_verified': True, 'diagnostic_only': True,
              'model': short_node.get('model', ''), 'harness': short_node.get('harness', ''),
              'sessionless': ga[-2] == 'sessionless', 'same_identity': ga == gb,
              'identity_differences': [name for name, x, y in zip(names, ga, gb) if x != y],
              'binding_issues': [ai, bi], 'message_counts': [len(a), len(b)],
              'common_stable_messages': common,
              'first_difference': difference(a[common], b[common]) if common < min(len(a), len(b)) else None,
              'literal_coverage': is_prefix(a, b),
              'full_view_coverage': {mode: [is_prefix(view(a, mode), view(b, mode)),
                                          is_prefix(view(b, mode), view(a, mode))]
                                     for mode in MODES if mode != 'cross_identity'},
              'normalization_issues': [[x.get('code') for x in (n.get('normalization_audit') or {}).get('issues', [])]
                                       for n in (short_node, long_node)]}
    return result


def sample_rank(row):
    return digest(dumps([row['short']['id'], row['long']['id'], row['mode']])).hex()


def bounded(a, b, maximum=5):
    values = {sample_rank(r): r for r in a + b}
    return [values[k] for k in sorted(values)[:maximum]]


def census(rows):
    result = Counter()
    for row in rows:
        for group in (row['ref']['dataset'], row['ref']['dataset'] + '/model=' + row['model']):
            result[group, 'records'] += 1
            result[group, 'without_session' if row['sessionless'] else 'with_session'] += 1
            result[group, 'messages'] += row['message_count']
            result[group, 'sub_ge_1000'] += int(row['sub_number'] >= 1000)
            for issue in row['binding_issues']:
                result[group, issue] += 1
    yield from result.items()
