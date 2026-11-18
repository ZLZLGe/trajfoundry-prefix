import copy
import hashlib

import audit_core as audit
import orjson
from explain_declared_cwd import declarations, relation
from survey_explicit_cwd import compare_members, split_command


def test_only_a_literal_single_absolute_prefix_is_recognized():
    assert split_command('cd /tmp/work && pwd') == ('cd /tmp/work && ', 'pwd')
    assert split_command('cd "/tmp/my work" && x') == ('cd "/tmp/my work" && ', 'x')
    for text in ('cd relative && pwd', 'cd ../work && pwd', 'cd /a/../b && pwd', 'cd "$DIR" && pwd',
                 'cd /x; pwd', 'cd /x && cd /b && pwd', 'cd /a &&\npwd', 'cd /x && '):
        assert split_command(text) is None


def test_incompatible_directories_are_distinct_and_neither_shell_command_runs(tmp_path):
    rows = []
    base = {'model': 'model', 'harness': 'test', 'metadata': {'session_id': 's'},
            'messages': [{'role': 'user', 'content': 'request'}]}
    for sub, cmd, tail in [(0, 'cd /a && pwd', False), (1, 'pwd', True), (2, 'cd /b && pwd', True)]:
        node = copy.deepcopy(base)
        node['messages'].append({'role': 'assistant', 'content': '', 'tool_calls': [
            {'type': 'function', 'id': 'call', 'function': {'name': 'Bash', 'arguments': {'command': cmd}}}]})
        if tail:
            node['messages'].append({'role': 'tool', 'name': 'Bash', 'tool_call_id': 'call', 'content': 'result'})
        raw = orjson.dumps(node)
        h = hashlib.sha256(raw).hexdigest()
        (tmp_path / h).write_bytes(raw)
        ref = {'sha256': h, 'bytes': len(raw), 'dataset': 'test', 'date': 'date'}
        rows.append({'ref': ref, 'sub': sub, 'scope_hash': hashlib.sha256(audit.dumps(audit.scope(node, ref))).hexdigest()})
    results = []
    counts = compare_members(rows, (tmp_path,), results.append)
    assert {(r['left'], r['right']) for r in results} == {(0, 1), (1, 2)}
    assert counts['distinct_shorter'] == 1 and counts['distinct_equal_members'] == 2


def test_only_readable_role_specific_declarations_are_used():
    seen = declarations([{'role': 'system', 'content': ' - Primary working directory: /a (was /b)'},
                         {'role': 'user', 'content': 'Primary working directory: /untrusted'},
                         {'role': 'tool', 'content': 'Shell cwd was reset to /c'}])
    assert seen == ('/a', '/c', '/c')
    assert relation({'/a'}, '/a') == 'all_equal'
    assert relation({'/a', '/b'}, '/a') == 'some_equal'
    assert relation({'/a/child', '/a/sibling'}, '/a') == 'all_descendants'
    assert relation({'/also'}, '/a') == 'other'
    assert relation({'/a'}, None) == 'no_line'
