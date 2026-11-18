from verify_sessionless_cwd import check_pair, evidence


def record(commands, *, user='u', prompt='task', history=False):
    ref = {'dataset': 'deep-infra', 'date': '2026-10-30'}
    calls = [{'id': f'call-{i}', 'type': 'function',
              'function': {'name': 'Bash', 'arguments': {'command': cmd}}} for i, cmd in enumerate(commands)]
    messages = [{'role': 'user', 'content': prompt}, {'role': 'assistant', 'content': '', 'tool_calls': calls}]
    if history:
        messages.extend({'role': 'tool', 'name': 'Bash', 'content': 'done', 'tool_call_id': call['id']}
                        for call in calls)
    return evidence({'metadata': {'user_id': user}, 'harness': 'unknown', 'model': 'test', 'messages': messages}, ref)


def test_only_the_omitted_cwd_in_a_batch_counts():
    short = record(['cd /a && pwd', 'cd /b && pwd'])
    reason, detail = check_pair(short, record(['pwd', 'cd /b && pwd'], history=True))
    assert reason == 'verified' and detail['changed_call_positions'] == [0] and detail['same_original_ids']
    assert check_pair(short, record(['pwd', 'cd /c && pwd'], history=True))[0] == 'different_command_or_arguments'


def test_different_user_or_earlier_prompt_blocks_a_terminal_match():
    short = record(['cd /a && pwd'])
    assert check_pair(short, record(['pwd'], user='other', history=True))[0] == 'different_identity_scope'
    assert check_pair(short, record(['pwd'], prompt='other', history=True))[0] == 'different_earlier_history_or_length'
