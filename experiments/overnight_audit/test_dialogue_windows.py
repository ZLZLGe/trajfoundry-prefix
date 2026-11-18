from survey_dialogue_windows import dialogue, find_windows


def msg(role, text):
    return {'role': role, 'content': text}


def assistant_call(identifier):
    return {**msg('assistant', ''), 'tool_calls': [
        {'id': identifier, 'type': 'function', 'function': {'name': 'read', 'arguments': {}}}]}


def result(identifier):
    return {**msg('tool', 'data'), 'tool_call_id': identifier, 'name': 'read'}


def item(messages, sub):
    return dialogue({'messages': [msg('system', 'instructions'), *messages]}, sub, 'same')


def test_shifted_window_preserves_binding_after_an_earlier_call():
    start = [msg('user', 'old'), assistant_call('prior'), result('prior')]
    seq = [msg('user', 'new'), assistant_call('next'), result('next'), msg('assistant', 'finished')]
    short = item(seq, 0)
    long = item(start + seq + [msg('user', 'one more')], 1)
    rows = list(find_windows([short, long]))
    assert len(rows) == 1 and rows[0]['skipped_dialogue_messages'] == 3
    assert rows[0]['trailing_dialogue_messages'] == 1 and rows[0]['original_call_ids_equal'] is True
    rebound = item(start + [*seq[:2], result('prior'), seq[3]], 1)
    assert not list(find_windows([short, rebound]))


def test_whole_short_dialogue_must_match_and_contain_both_roles():
    first = [msg('user', 'init'), msg('assistant', 'ok')]
    seq = [msg('user', 'q'), msg('assistant', 'answer')]
    changed = item(first + [seq[0], msg('developer', 'change'), seq[1]], 1)
    assert not list(find_windows([item(seq, 0), changed]))
    assert not list(find_windows([item(seq[1:], 0), item(first + seq, 1)]))
    assert not list(find_windows([item(seq, 0), item(seq + first, 1)]))
    found = list(find_windows([item(seq, 0), item(first + seq, 1)]))
    assert len(found) == 1 and found[0]['trailing_dialogue_messages'] == 0


def test_different_initial_instructions_are_reported_separately():
    seq = [msg('user', 'q'), msg('assistant', 'answer')]
    prefix = [msg('system', 'other instructions'), msg('user', 'old'), msg('assistant', 'done')]
    long = dialogue({'messages': prefix + seq}, 1, 'same')
    found = list(find_windows([item(seq, 0), long]))
    assert len(found) == 1 and found[0]['same_headers'] is False
