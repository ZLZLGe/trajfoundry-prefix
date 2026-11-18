from check_identity_replays import prefix_features, time_bucket


def interaction(identifier, tail=False):
    msgs = [{'role': 'user', 'content': 'query'},
            {'role': 'assistant', 'content': '', 'tool_calls': [
                {'id': identifier, 'type': 'function', 'function': {'name': 'read', 'arguments': {'x': 7}}}]},
            {'role': 'tool', 'name': 'read', 'tool_call_id': identifier, 'content': 'result'}]
    if tail:
        msgs.append({'role': 'assistant', 'content': 'done'})
    return {'messages': msgs}


def test_original_ids_are_checked_separately_from_visible_coverage_at_requested_length():
    left, n, _ = prefix_features(interaction('raw-id'), {3})
    right, m, _ = prefix_features(interaction('rewritten-id', tail=True), {3, 4})
    assert (n, m) == (3, 4)
    assert left[3][0] == right[3][0] and left[3][1] != right[3][1]
    assert left[3][2:] == right[3][2:] == (1, 0)
    assert left[3][0] != right[4][0]


def test_time_windows_keep_reverse_equal_and_missing_separate():
    assert [time_bucket(t) for t in (None, -.1, 0, .001, 60, 60.1, 300, 301)] == [
        'missing', 'compared_longer_earlier', 'same_timestamp', 'later_0_to_60_seconds',
        'later_0_to_60_seconds', 'later_1_to_5_minutes', 'later_1_to_5_minutes', 'later_over_5_minutes']
