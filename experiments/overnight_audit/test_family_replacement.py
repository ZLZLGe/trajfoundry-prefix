from explain_family_replacement_characters import BROKEN, characters, compare_characters


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
