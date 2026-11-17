import random
import re
import subprocess
import sys
from copy import deepcopy

import pytest
from test_tool_ids import sample

from trajectory_prefix.pair_review import MASK, argument_view, masked_string_candidate, review_pair
from trajectory_prefix.projection import Policy

POLICY = Policy(extra_information="messages_only")
REF = {"dataset":"test", "date":"2026-10-10"}


def test_redaction_probe_does_not_claim_safe_coverage_or_mutate_inputs():
    a, b = sample("a", True), sample("b", True)
    a["messages"][2]["content"] = "written to jobs/<ID_NUMBER>/data.py"
    b["messages"][2]["content"] = "written to jobs/abc123/data.py"
    originals = deepcopy((a, b))
    result = review_pair(a, b, REF, REF, POLICY)
    assert not any(result["current_covers"])
    assert not any(result["independent_current_covers"])
    assert result["diagnostic_prefix_candidates"]["redaction_placeholder_candidates"] == [True, True]
    assert (a, b) == originals


def test_redaction_probe_requires_entire_remaining_prefix_to_match():
    a, b = sample("a", True), sample("b", True)
    a["messages"][2]["content"] = "hello <PERSON>"
    b["messages"][2]["content"] = "hello Alice"
    b["messages"][3]["content"] = "different task conclusion"
    result = review_pair(a, b, REF, REF, POLICY)
    assert "redaction_placeholder_candidates" not in result["diagnostic_prefix_candidates"]
    assert len(result["position_differences"]) == 2


def test_scope_differences_stay_separate_even_when_messages_are_equal():
    a, b = sample("a", True), sample("b", True)
    b["metadata"]["session_id"] = "different"
    result = review_pair(a, b, REF, REF, POLICY)
    assert not result["same_scope"]
    assert result["scope_differences"] == ["session_or_date"]
    assert result["message_prefix_without_scope"] == [True, True]
    assert result["current_covers"] == [False, False]


@pytest.mark.parametrize("a,b", [
    ('{"a":1,"b":2}', '{ "b": 2, "a": 1 }'),
    ('{"cmd":"pwd"}', {"cmd":"pwd"}),
])
def test_argument_probe_accepts_only_lossless_encoding_changes(a, b):
    assert argument_view(a) == argument_view(b)


@pytest.mark.parametrize("a,b", [
    ('{"a":1,"a":2}', '{"a":2}'),
    ('{"a":true}', '{"a":1}'),
    ('{"a":1.00000000000000001}', '{"a":1.00000000000000002}'),
    ('{"a":NaN}', '{"a":null}'),
])
def test_argument_probe_does_not_hide_semantic_or_ambiguous_changes(a, b):
    assert argument_view(a) != argument_view(b)


def test_omitted_background_notification_is_only_a_candidate():
    a, b = sample("a", True), sample("b", True)
    a["messages"].insert(3, {"role":"user", "content":"<task-notification>done</task-notification>"})
    result = review_pair(a, b, REF, REF, POLICY)
    assert not any(result["current_covers"])
    assert result["diagnostic_prefix_candidates"]["without_notifications"] == [True, True]


def test_phone_mask_in_numeric_output_is_a_candidate_but_not_safe_coverage():
    a, b = sample("a", True), sample("b", True)
    a["messages"][2]["content"] = "dist <PHONE>; goal [1.<PHONE>.1986069]"
    b["messages"][2]["content"] = "dist 4.173200071891493; goal [1.06254773 2.1986069]"
    result = review_pair(a, b, REF, REF, POLICY)
    assert not any(result["current_covers"])
    assert not any(result["independent_current_covers"])
    assert result["diagnostic_prefix_candidates"]["redaction_placeholder_candidates"] == [True, True]
    assert not masked_string_candidate("result <UNKNOWN>", "result changed")


def test_combined_diagnostic_keeps_real_content_changes_visible():
    a, b = sample("a", True), sample("b", True)
    a["messages"].insert(0, {"role":"system", "content":"one instruction"})
    b["messages"].insert(0, {"role":"system", "content":"another instruction"})
    a["messages"][3]["content"] = "dist <PHONE>"
    b["messages"][3]["content"] = "dist 4.173200071891493"
    a["messages"].insert(4, {"role":"user", "content":"<task-notification>done</task-notification>"})
    originals = deepcopy((a, b))
    result = review_pair(a, b, REF, REF, POLICY)
    assert not any(result["current_covers"])
    assert set(result["diagnostic_prefix_candidates"]) == {
        "redaction_without_headers_and_notifications"
    }
    assert (a, b) == originals
    b["messages"][-1]["content"] = "a genuinely different answer"
    assert not review_pair(a, b, REF, REF, POLICY)["diagnostic_prefix_candidates"]


@pytest.mark.parametrize("pattern,value,expected", [
    ("a<PERSON>z", "az", False),
    ("a<PERSON>z", "a" + "x" * 256 + "z", True),
    ("a<PERSON>z", "a" + "x" * 257 + "z", False),
    ("<PERSON><PHONE>", "x", False),
    ("<PERSON><PHONE>", "xy", True),
    ("<PERSON><PHONE>", "x" * 512, True),
    ("<PERSON><PHONE>", "x" * 513, False),
    ("a<PERSON>z", "ax\nyz", False),
    ("a<PERSON>z", "ax\ryz", False),
    ("<PERSON>\r\n<PHONE>", "甲\r\n乙", True),
    ("<PERSON>\n<PHONE>", "甲\n乙\n丙", False),
    ("<PERSON>a<PHONE>", "aax", True),
    ("<PERSON>a<PHONE>", "xa", False),
    ("a<UNKNOWN>z", "axyz", False),
])
def test_mask_matching_preserves_character_bounds_and_literal_newlines(pattern, value, expected):
    assert masked_string_candidate(pattern, value) is expected
    assert masked_string_candidate(value, pattern) is expected


def test_mask_matching_agrees_with_previous_regex_on_bounded_synthetic_inputs():
    def oracle(a, b):
        if a == b:
            return True
        for pattern, value in ((a, b), (b, a)):
            parts = MASK.split(pattern)
            if len(parts) > 1:
                regex = r"[^\r\n]{1,256}?".join(map(re.escape, parts))
                if re.fullmatch(regex, value):
                    return True
        return False

    randomizer = random.Random(20261117)
    pieces = ["a", "b", " ", "\r", "\n", "甲", "<PERSON>", "<PHONE>"]
    values = ["a", "bb", " ", "\n", "甲乙", "<PERSON>"]
    for _ in range(3000):
        a = "".join(randomizer.choices(pieces, k=randomizer.randint(1, 6)))
        b = MASK.sub(lambda _: randomizer.choice(values), a)
        if randomizer.randrange(2):
            b += randomizer.choice(values)
        assert masked_string_candidate(a, b) == oracle(a, b), (a, b)


def test_repeated_mask_late_mismatch_finishes_without_exponential_backtracking():
    # A subprocess bounds regression runtime even if the old regex returns.
    code = (
        "from trajectory_prefix.pair_review import masked_string_candidate as match; "
        "assert not match('<PERSON> ' * 14 + 'MISSING <PHONE> END', 'x ' * 60 + 'x END'); "
        "assert match('<PERSON>' * 64, 'x' * 4096)"
    )
    subprocess.run([sys.executable, "-c", code], check=True, capture_output=True, timeout=5)
