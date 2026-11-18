from survey_terminal_tool_text import inspect_family


def record(sub, text, *, longer=False, description='run', identifier='id', opening='question', scope='s'):
    messages = [{'role': 'user', 'content': opening},
                {'role': 'assistant', 'content': text, 'tool_calls': [
                    {'id': identifier, 'type': 'function', 'function': {'name': 'tool', 'arguments': description}}]}]
    if longer:
        messages.append({'role': 'tool', 'content': 'ok', 'name': 'tool', 'tool_call_id': identifier})
    return sub, scope, {'messages': messages}


def test_accepts_only_exact_raw_call_and_agreeing_earlier_messages_in_one_scope():
    rows = [record(0, 'original commentary'), record(1, '', longer=True), record(2, '', longer=True, scope='t'),
            record(3, '', longer=True, opening='different'), record(4, '', longer=True, identifier='other'),
            record(5, '', longer=True, description='other'), record(6, 'new commentary', longer=True),
            record(7, '')]
    found = []
    report = inspect_family(rows, found.append)
    assert {(r['short'], r['long']) for r in found} == {(0, 1), (7, 6)}
    assert report['pairs'] == {'short_with_text': 1, 'long_with_text': 1}
    assert report['union_distinct_shorts'] == 2
