import pytest
from index_edit_defaults import folded_history, optional_false
from verify_edit_defaults import comparable, matches, prove


def node(args, *, more=False, required=False, default=False, name='Edit'):
    schema = {'properties': {'replace_all': {'type': 'boolean', 'default': default}},
              'required': ['file_path'] + (['replace_all'] if required else [])}
    messages = [{'role': 'user', 'content': 'edit'},
                {'role': 'assistant', 'content': '', 'tool_calls': [
                    {'id': 'one', 'type': 'function', 'function': {'name': name, 'arguments': args}}]}]
    if more:
        messages.append({'role': 'tool', 'name': name, 'content': 'ok', 'tool_call_id': 'one'})
    return {'tools': [{'name': name, 'parameters': schema}], 'messages': messages}


def test_optional_false_and_omitted_on_either_side():
    short = comparable(node({'file_path': 'a'}))
    long = comparable(node({'file_path': 'a', 'replace_all': False}, more=True))
    a, x = folded_history(short[0])
    b, y = folded_history(long[0])
    assert not b.startswith(a) and y.startswith(x)
    changes, length = prove(short, long)
    assert length == [2, 3] and changes == [{'position': 1, 'call': 0, 'false_on': 'long', 'same_original_id': True}]
    for other in (True, 'false', 0, None):
        changed = comparable(node({'file_path': 'a', 'replace_all': other}, more=True))
        assert not folded_history(changed[0])[1].startswith(x)
        with pytest.raises(AssertionError):
            prove(short, changed)


def test_definition_is_required_on_both_sides_and_other_arguments_count():
    assert not optional_false(node({}, required=True))
    assert not optional_false(node({}, default=0))
    assert not optional_false(node({}, name='Other'))
    n = node({})
    n['tools'] *= 2
    assert not optional_false(n)
    with pytest.raises(AssertionError):
        prove(comparable(node({'file_path': 'b'})), comparable(node({'file_path': 'a', 'replace_all': False})))


def test_sorted_prefix_walk_keeps_equal_ties_and_all_nonadjacent_ancestors():
    rows = [{'folded': s, 'original': a, 'path': str(i)} for i, (s, a) in enumerate([
        (b'ab', b'xb'), (b'a', b'z'), (b'a', b'a'), (b'a', b'z'), (b'acc', b'acc'), (b'b', b'b')])]
    order = sorted(rows, key=lambda r: (r['folded'], r['path']))
    expected = {(a['path'], b['path']) for i, a in enumerate(order) for b in order[i + 1:]
                if b['folded'].startswith(a['folded']) and not b['original'].startswith(a['original'])}
    assert {(a['path'], b['path']) for a, b in matches(rows)} == expected
