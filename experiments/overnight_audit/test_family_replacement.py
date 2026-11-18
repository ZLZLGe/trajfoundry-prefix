from explain_family_replacement_characters import (
    BROKEN,
    characters,
    compare_characters,
    compare_family,
)


def compare(left, right):
    return compare_characters(characters(left), characters(right))


def test_one_observed_character_is_recorded_in_both_directions():
    for width in range(1, 5):
        text = 'pre' + BROKEN * width + 'post'
        assert compare(text, 'pre\u8003post') == [('left', 3, width, '\u8003')]
        assert compare('pre\u8003post', text) == [('right', 3, width, '\u8003')]


def test_two_damaged_positions_keep_the_intervening_literal_text():
    left = BROKEN * 2 + 'x' + BROKEN * 3
    assert compare(left, '\u8003x\u70b9') == [('left', 0, 2, '\u8003'), ('left', 2, 3, '\u70b9')]
    assert compare(left, '\u8003y\u70b9') is None


def test_disallowed_substitutions():
    for observed in ('A', '7', ' ', '\t', '\r', '\n', '\u8003\u70b9', ''):
        assert compare(BROKEN * 2, observed) is None
    assert compare(BROKEN * 5, '\u8003') is None
    assert compare(BROKEN * 2, BROKEN * 3) is None
    assert compare('\u8003', '\u70b9') is None
    assert compare(BROKEN * 2 + 'x', BROKEN * 2 + 'x') == []


def test_one_damaged_text_does_not_make_two_concrete_options_equal():
    damaged = 'a' + BROKEN * 2
    assert compare(damaged, 'a\u8003')
    assert compare(damaged, 'a\u70b9')
    assert compare('a\u8003', 'a\u70b9') is None


def test_family_search_honors_scope_exact_suffix_and_equal_length_orientation():
    def member(sub, scope, text, suffix=None):
        messages = [{'role': 'user', 'content': text}]
        if suffix is not None:
            messages.append({'role': 'assistant', 'content': suffix})
        return sub, scope, {'messages': messages}

    rows = [member(0, 'a', BROKEN * 2), member(1, 'a', '\u8003'), member(2, 'a', '\u8003', 'done'),
            member(3, 'b', '\u8003', 'done'), member(4, 'a', BROKEN * 2, 'other')]
    witnesses = []
    counts = compare_family(rows, witnesses.append)
    assert counts['distinct_strict_shorter_records'] == 2
    assert counts['distinct_equal_length_members'] == 2
    assert counts['pair_counts']['already_literal_visible_prefix'] == 2
    assert {(w['left_sub'], w['right_sub']) for w in witnesses} == {(0, 1), (0, 2), (1, 4)}
