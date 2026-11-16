from copy import deepcopy

from test_tool_ids import call, sample

from trajectory_prefix.projection import Policy, covers, project
from trajectory_prefix.verification import raw_covers

REF = {"dataset": "example", "date": "archive"}


def test_direct_binding_verifier_checks_real_result_associations():
    a, b = sample("old", True), sample("new", True)
    assert raw_covers(a, b, REF, REF)
    assert not raw_covers(a, b, REF, REF, Policy(()))
    b["messages"][1]["tool_calls"].append(call("other"))
    b["messages"][2]["tool_call_id"] = "other"
    assert not raw_covers(a, b, REF, REF)


def test_raw_oracle_matches_projection_across_rules_and_boundary_mutations():
    a = sample("a", True)
    a["messages"].insert(
        0,
        {
            "role": "system",
            "content": "x-anthropic-billing-header: cch=123abc;\nFollow instructions.",
        },
    )
    a["messages"][2]["content"] = "\n\n"
    b = sample("b", True)
    b["messages"].insert(
        0,
        {
            "role": "system",
            "content": "x-anthropic-billing-header: cch=456def;\nFollow instructions.",
        },
    )
    cases = [b]
    for edit in [
        lambda n: n["messages"][3].update(tool_call_id="wrong"),
        lambda n: n["messages"][3].update(content="wrong"),
        lambda n: n["messages"][1].update(content="another question"),
        lambda n: n.update(extra_evidence={"unique": True}),
        lambda n: n.update(tools=[{"type": "function", "function": {"name": "extra"}}]),
        lambda n: n["metadata"].update(user_id="other"),
    ]:
        n = deepcopy(b)
        edit(n)
        cases.append(n)
    rules = Policy().rules
    for bitset in range(8):
        policy = Policy(tuple(rule for i, rule in enumerate(rules) if bitset & (1 << i)))
        for n in cases:
            expected = covers(
                project(a, "example", "archive", policy), project(n, "example", "archive", policy)
            )
            assert raw_covers(a, n, REF, REF, policy) == expected
            reverse = covers(
                project(n, "example", "archive", policy), project(a, "example", "archive", policy)
            )
            assert raw_covers(n, a, REF, REF, policy) == reverse
