import copy
import itertools

import audit_core as audit


def node(text='inspect', identifier='call-a'):
    return {'model': 'test', 'harness': 'agent', 'metadata': {'session_id': 'session-a'},
            'messages': [{'role': 'user', 'content': text},
                         {'role': 'assistant', 'content': '', 'reasoning_content': 'think',
                          'tool_calls': [{'id': identifier, 'type': 'function',
                                          'function': {'name': 'exec', 'arguments': {'cmd': 'ls'}}}]},
                         {'role': 'tool', 'tool_call_id': identifier, 'name': 'exec', 'content': 'a.txt'},
                         {'role': 'assistant', 'content': 'done'}]}


def ref(i):
    return {'id': str(i), 'dataset': 'test', 'date': '2026-10-30', 'path': f's_sub_{i}.jsonl', 'bytes': 100}


def test_independent_view_keeps_input_and_reasoning_only_positions():
    a = node()
    before = copy.deepcopy(a)
    b = node(identifier='rewritten')
    b['messages'][1]['reasoning_content'] = 'different thought'
    assert audit.stable_messages(a)[0] == audit.stable_messages(b)[0]
    b['messages'].insert(3, {'role': 'assistant', 'content': '', 'reasoning_content': 'only reasoning'})
    assert len(audit.stable_messages(b)[0]) == 5
    assert a == before


def test_changed_bindings_remain_distinct_and_flagged():
    a, b = node(), node()
    b['messages'][2]['tool_call_id'] = 'unknown'
    assert audit.stable_messages(a)[0] != audit.stable_messages(b)[0]
    assert audit.stable_messages(b)[1] == ['unresolved_tool_result']


def test_neighbor_scan_retains_every_covered_record_with_one_witness():
    values = []
    for i, seq in enumerate([('a',), ('a', 'b'), ('a', 'b'), ('a', 'c'), ('d', 'e'), ('d', 'e', 'f')]):
        vector = b''.join(audit.digest(x.encode()) for x in seq)
        values.append((('x', b'scope', vector, -100, str(i)), {'ref': ref(i), 'scope': b'scope'}))
    rows = sorted(values)
    actual = list(audit.neighbors(rows))
    expected_prefixes = {a[1]['ref']['id'] for a, b in itertools.permutations(rows, 2)
                         if len(a[0][2]) < len(b[0][2]) and b[0][2].startswith(a[0][2])}
    equal_removed = {b[1]['ref']['id'] for a, b in itertools.combinations(rows, 2) if a[0][2] == b[0][2]}
    assert {r['short']['id'] for r in actual} == expected_prefixes | equal_removed


def test_format_probe_is_diagnostic_and_validates_full_tail():
    a, b = node(' task\r\n'), node('task\n')
    am, _ = audit.stable_messages(a)
    bm, _ = audit.stable_messages(b)
    assert not audit.is_prefix(am, bm)
    assert audit.is_prefix(audit.view(am, 'text_format'), audit.view(bm, 'text_format'))
    b['messages'][-1]['content'] = 'different conclusion'
    bm, _ = audit.stable_messages(b)
    assert not audit.is_prefix(audit.view(am, 'text_format'), audit.view(bm, 'text_format'))


def test_identity_probe_keeps_model_and_harness_and_reports_scope_change():
    a, b = node(), node()
    b['metadata']['session_id'] = 'different'
    ar, br = ref(1), ref(2)
    evidence = audit.verify({'short': ar, 'long': br, 'same_identity': False,
                             'mode': 'cross_identity', 'kind': 'coverage'}, a, b)
    assert evidence['identity_differences'] == ['session_or_date']
    assert evidence['diagnostic_only'] is True


def test_notice_probe_keeps_unrelated_user_instructions():
    a = [{'role': 'user', 'content': 'solve A<system-reminder>runtime text</system-reminder>'}]
    assert audit.view(a, 'without_notices') == [{'role': 'user', 'content': 'solve A'}]
    assert audit.view([{'role': 'user', 'content': 'solve B'}], 'without_notices') != audit.view(a, 'without_notices')


def test_empty_visible_probe_reveals_omitted_thought_turn_without_changing_baseline():
    a, b = node(), node()
    a['messages'].insert(3, {'role': 'assistant', 'content': '', 'reasoning_content': 'private thought'})
    am, _ = audit.stable_messages(a)
    bm, _ = audit.stable_messages(b)
    assert len(am) == 5 and len(bm) == 4
    assert not audit.is_prefix(am, bm)
    assert audit.is_prefix(audit.view(am, 'without_empty_visible_assistant'),
                           audit.view(bm, 'without_empty_visible_assistant'))
    assert len(audit.view(am, 'stable_messages')) == 5


def test_attribution_distinguishes_missing_user_and_redacted_identity():
    import attribution_core
    a = node()
    a['messages'][0]['role'] = 'system'
    b = copy.deepcopy(a)
    row = {'short': ref(1), 'long': ref(2), 'modes': ['stable_messages']}
    result = attribution_core.explain(row, a, b)
    assert result['reason'] == 'protected:no_user_assistant_exchange'
    a, b = node(), node()
    a['normalization_audit'] = {'issues': [{'code': 'metadata_session_id_masked'}]}
    result = attribution_core.explain(row, a, b)
    assert result['reason'] == 'protected:redacted_identity'


def test_attribution_recognizes_leading_space_without_changing_policy():
    import attribution_core
    a, b = node(), node()
    a['messages'][-1]['content'] = '\n\ndone'
    row = {'short': ref(1), 'long': ref(2), 'modes': ['text_format']}
    result = attribution_core.explain(row, a, b)
    assert result['reason'] == 'assistant_leading_whitespace_only'
    assert not result['production_covers'] and not result['production_raw_covers']
    assert a['messages'][-1]['content'] == '\n\ndone'


def test_attribution_reports_cross_scope_and_existing_rule_residual():
    import attribution_core
    a, b = node(), node()
    row = {'short': ref(1), 'long': ref(2), 'modes': ['cross_identity']}
    assert attribution_core.explain(row, a, b)['reason'] == 'covered_by_unchanged_production_rule'
    b['metadata']['session_id'] = 'another-session'
    result = attribution_core.explain(row, a, b)
    assert result['reason'] == 'identity_boundary'
    assert result['identity_differences'] == ['session_or_date']


def test_reasoning_only_census_includes_whitespace_visible_output():
    import attribution_core
    a = node()
    a['messages'].extend([{'role': 'assistant', 'content': '\n\n', 'reasoning_content': 'thinking'},
                          {'role': 'assistant', 'content': '  '}])
    assert attribution_core.empty_counts(a) == {'reasoning_only': 1, 'fully_empty': 1}


def test_structure_probe_can_find_mid_history_notice_without_erasing_user():
    a, b = node(), node()
    b['messages'].insert(2, {'role': 'system', 'content': 'The Bash tool shell is Git Bash.'})
    am, _ = audit.stable_messages(a)
    bm, _ = audit.stable_messages(b)
    assert not audit.is_prefix(am, bm)
    assert audit.is_prefix(audit.view(am, 'without_system_developer'),
                           audit.view(bm, 'without_system_developer'))
    b['messages'][0]['content'] = 'a different user instruction'
    bm, _ = audit.stable_messages(b)
    assert not audit.is_prefix(audit.view(am, 'without_system_developer'),
                               audit.view(bm, 'without_system_developer'))
