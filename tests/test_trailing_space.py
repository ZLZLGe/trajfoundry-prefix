from copy import deepcopy

import pytest

from trajectory_prefix.projection import Policy, covers, project


def sample(content, role="assistant"):
    return {
        "metadata": {"session_id": "s"},
        "model": "m",
        "messages": [
            {"role": "user", "content": "task"},
            {"role": role, "content": content},
        ],
    }


@pytest.mark.parametrize(
    "before,after", [("\n\n", ""), ("answer \n\t", "answer"), ("a\nb\n", "a\nb")]
)
def test_assistant_ascii_suffix_is_comparison_only(before, after):
    a, b = sample(before), sample(after)
    original = deepcopy([a, b])
    assert not covers(project(a, "x", "d", Policy(())), project(b, "x", "d", Policy(())))
    assert covers(project(a, "x", "d"), project(b, "x", "d"))
    assert [a, b] == original


@pytest.mark.parametrize(
    "before,after,role",
    [
        ("a\n b", "a\nb", "assistant"),
        (" answer", "answer", "assistant"),
        ("answer\u00a0", "answer", "assistant"),
        ("answer\n", "answer", "user"),
        ("result\n", "result", "tool"),
    ],
)
def test_interior_leading_unicode_and_nonassistant_whitespace_is_protected(before, after, role):
    assert not covers(
        project(sample(before, role), "x", "d"), project(sample(after, role), "x", "d")
    )
