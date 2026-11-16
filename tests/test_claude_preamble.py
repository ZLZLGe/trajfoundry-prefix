from copy import deepcopy
from dataclasses import replace

import pytest
from test_billing_header import sample

from trajectory_prefix.projection import Policy, covers, project
from trajectory_prefix.verification import raw_covers

INTRO = (
    "You are Claude Code, Anthropic's official CLI for Claude. Model: Example.\n"
    "Keep the actual identity instructions unchanged."
)
BILLING = "x-anthropic-billing-header: cc_version=2.1.161.98c; cc_entrypoint=cli; cch=73ab5;"
TEXT = INTRO + "\n\n" + BILLING + "\nActual task instructions."
REF = {"dataset": "example", "date": "2026-10-12"}


def relation(left, right, expected, policy=Policy()):
    before = deepcopy((left, right))
    assert (
        covers(
            project(left, "example", REF["date"], policy),
            project(right, "example", REF["date"], policy),
        )
        is expected
    )
    assert raw_covers(left, right, REF, REF, policy) is expected
    assert (left, right) == before


@pytest.mark.parametrize("role", ["system", "developer"])
def test_billing_after_first_claude_identity_paragraph_preserves_everything_else(role):
    left, right = sample(TEXT), sample(TEXT.replace("73ab5", "73004"))
    left["messages"][0]["role"] = right["messages"][0]["role"] = role
    right["messages"] += [
        {"role": "user", "content": "continue"},
        {"role": "assistant", "content": "completed"},
    ]
    relation(left, right, True)
    old = replace(Policy(), rules=tuple(r for r in Policy().rules if r != "claude_preamble_cch"))
    relation(left, right, False, old)
    relation(left, right, True, replace(Policy(), rules=("claude_preamble_cch",)))


@pytest.mark.parametrize(
    "before,after",
    [
        ("Model: Example", "Model: Other"),
        ("identity instructions unchanged", "identity instructions changed"),
        ("cc_version=2.1.161.98c", "cc_version=2.1.162"),
        ("cc_entrypoint=cli", "cc_entrypoint=sdk"),
        ("Actual task instructions", "Different task instructions"),
    ],
)
def test_changed_preamble_version_entrypoint_or_task_is_never_ignored(before, after):
    relation(sample(TEXT), sample(TEXT.replace("73ab5", "73004").replace(before, after)), False)


@pytest.mark.parametrize(
    "text",
    [
        "An example follows.\n" + TEXT,
        INTRO + "\n\nAn example follows.\n" + BILLING,
        INTRO + "\n```example\n\n" + BILLING,
        INTRO + "\n~~~example\n\n" + BILLING,
        TEXT.replace("cc_version=2.1.161.98c", "cc_version=2:cch=123"),
        TEXT.replace("cc_entrypoint=cli", "cc_entrypoint=cli:cch=123"),
        TEXT.replace("cch=73ab5;", "cch=73ab5z;"),
        TEXT.replace("cch=73ab5;", "cch=73ab5; explanation"),
    ],
)
def test_only_the_structured_header_immediately_after_the_identity_paragraph_is_eligible(text):
    relation(sample(text), sample(text.replace("73ab5", "73004")), False)


def test_user_content_and_later_quoted_billing_lines_are_preserved():
    left, right = sample(TEXT), sample(TEXT.replace("73ab5", "73004"))
    left["messages"][0]["role"] = right["messages"][0]["role"] = "user"
    relation(left, right, False)
    quoted = TEXT + "\nAn example:\n" + BILLING
    relation(sample(quoted), sample(quoted.replace("73ab5", "73004")), False)
