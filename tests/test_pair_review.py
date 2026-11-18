from copy import deepcopy

import pytest
from test_tool_ids import sample

from trajectory_prefix.pair_review import argument_view, review_pair
from trajectory_prefix.projection import Policy

POLICY = Policy(extra_information="messages_only")
REF = {"dataset":"test", "date":"2026-10-10"}


def test_placeholder_difference_is_literal_in_all_pair_probes():
    a, b = sample("a", True), sample("b", True)
    a["messages"][2]["content"] = "written to jobs/<ID_NUMBER>/data.py"
    b["messages"][2]["content"] = "written to jobs/abc123/data.py"
    originals = deepcopy((a, b))
    result = review_pair(a, b, REF, REF, POLICY)
    assert not any(result["current_covers"])
    assert not any(result["independent_current_covers"])
    assert not result["diagnostic_prefix_candidates"]
    assert all("redaction_candidate" not in d for d in result["first_difference"]["details"])
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


def test_phone_mask_in_numeric_output_stays_distinct():
    a, b = sample("a", True), sample("b", True)
    a["messages"][2]["content"] = "dist <PHONE>; goal [1.<PHONE>.1986069]"
    b["messages"][2]["content"] = "dist 4.173200071891493; goal [1.06254773 2.1986069]"
    result = review_pair(a, b, REF, REF, POLICY)
    assert not any(result["current_covers"])
    assert not any(result["independent_current_covers"])
    assert not result["diagnostic_prefix_candidates"]


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
    assert not result["diagnostic_prefix_candidates"]
    assert (a, b) == originals
    b["messages"][-1]["content"] = "a genuinely different answer"
    assert not review_pair(a, b, REF, REF, POLICY)["diagnostic_prefix_candidates"]
