from copy import deepcopy

from trajectory_prefix.engine import Record, deduplicate
from trajectory_prefix.projection import Policy, covers, project


def node(messages, **kwargs):
    return dict(
        messages=messages,
        metadata={"session_id": "s", "user_id": "u"},
        model="model",
        harness="agent",
        tools=[],
        **kwargs,
    )


def user(text="question"):
    return {"role": "user", "content": text}


def assistant(text="answer"):
    return {"role": "assistant", "content": text, "reasoning_content": ""}


def record(name, value):
    return Record({"id": name, "bytes": 100}, project(value, "source", "2026-01-01"))


def test_prefix_branches_and_transitive_final_witness(tmp_path):
    a = node([user(), assistant()])
    b = node(a["messages"] + [user("next"), assistant("middle")])
    c = node(b["messages"] + [user("last"), assistant("final")])
    d = node(a["messages"] + [user("branch"), assistant("different")])
    rows = deduplicate([record(k, v) for k, v in zip("abcd", (a, b, c, d))], tmp_path)
    assert {r["record"]["id"] for r in rows if r["status"] == "kept"} == {"c", "d"}
    for r in rows:
        if r["status"] == "removed":
            assert r["retained"]["id"] in {"c", "d"}


def test_extra_information_mode_keeps_tools_required(tmp_path):
    strict = Policy(extra_information="preserve_all")
    a = node(
        [user(), assistant()],
        sub_agent_trajectory={"child": {"messages": [assistant("important")]}},
    )
    b = node([user(), assistant(), user("next"), assistant("done")])
    assert covers(project(a, "x", "d"), project(b, "x", "d"))
    assert not covers(project(a, "x", "d", strict), project(b, "x", "d", strict))
    a.pop("sub_agent_trajectory")
    a["tools"] = [{"name": "important", "parameters": {}}]
    assert not covers(project(a, "x", "d"), project(b, "x", "d"))
    b["tools"] = deepcopy(a["tools"])
    assert covers(project(a, "x", "d"), project(b, "x", "d"))
    a["custom_evidence"] = "must retain"
    assert covers(project(a, "x", "d"), project(b, "x", "d"))
    assert not covers(project(a, "x", "d", strict), project(b, "x", "d", strict))


def test_identity_model_content_and_empty_captures_do_not_merge():
    a = node([user(), assistant()])
    for edit in [
        lambda b: b["metadata"].update(session_id="other"),
        lambda b: b["metadata"].update(user_id="other"),
        lambda b: b.update(model="other"),
        lambda b: b["messages"][0].update(content="different"),
    ]:
        b = deepcopy(a)
        edit(b)
        assert not covers(project(a, "x", "d"), project(b, "x", "d"))
    assert not covers(project(node([]), "x", "d"), project(a, "x", "d"))


def test_reasoning_is_comparison_only_and_input_is_unchanged(tmp_path):
    a = node([user(), assistant()])
    b = deepcopy(a)
    b["messages"][-1]["reasoning_content"] = "response-only reasoning"
    originals = deepcopy([a, b])
    rows = deduplicate([record("a", a), record("b", b)], tmp_path)
    assert sum(r["status"] == "kept" for r in rows) == 1
    assert [a, b] == originals


def test_sha_collision_is_not_prefix_proof(tmp_path, monkeypatch):
    import trajectory_prefix.projection as p

    monkeypatch.setattr(p, "digest", lambda _: b"x" * 32)
    rows = deduplicate(
        [
            record("a", node([user(), assistant("a")])),
            record("b", node([user(), assistant("b"), user("next"), assistant()])),
        ],
        tmp_path,
    )
    assert all(r["status"] == "kept" for r in rows)


def test_anonymous_short_exchange_is_isolated():
    a = node([user(), assistant()])
    a["metadata"] = {}
    assert not project(a, "x", "d", Policy()).mergeable


def test_scan_matches_all_pairs_maximality_with_branches_and_auxiliary_variants(tmp_path):
    import random

    from trajectory_prefix.verification import raw_covers

    rng = random.Random(20261117)
    raw = {}
    ref = {"dataset": "source", "date": "2026-01-01"}
    for i in range(120):
        messages = [user("question " + str(rng.randrange(3))), assistant("answer")]
        for depth in range(rng.randrange(4)):
            messages += [user("next " + str(depth)), assistant("branch " + str(rng.randrange(2)))]
        n = node(messages, custom_guard=rng.randrange(2))
        n["tools"] = [{"name": "tool" + str(k)} for k in range(rng.randrange(3))]
        raw[str(i)] = n
    rows = deduplicate([record(k, n) for k, n in raw.items()], tmp_path)
    kept = {r["record"]["id"] for r in rows if r["status"] == "kept"}
    for row in rows:
        key = row["record"]["id"]
        if row["status"] == "removed":
            target = row["retained"]["id"]
            assert target in kept
            assert raw_covers(raw[key], raw[target], ref, ref)
        else:
            assert not any(raw_covers(raw[key], raw[other], ref, ref) for other in kept - {key})
