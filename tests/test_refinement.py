import itertools
import random

from test_masking import indexed
from test_tool_ids import sample

from trajectory_prefix.mask_job import POLICY
from trajectory_prefix.mask_verification import raw_relaxed_covers, string_oracle
from trajectory_prefix.masking import (
    bounds_possible,
    group_candidates,
    possible_pair,
    priority,
    string_bound,
    string_matches,
)
from trajectory_prefix.refinement_job import compose, direct_audit
from trajectory_prefix.storage import StorageConfig


def test_selective_candidates_equal_exhaustive_filter_with_shorter_histories(tmp_path):
    rng = random.Random(30)
    nodes = []
    for i in range(60):
        n = sample(str(i), True)
        n["messages"][0]["content"] = rng.choice(["a", "b", "<PERSON>"])
        n["messages"][2]["content"] = rng.choice(["x", "y", "[SENSITIVE]"])
        if i % 3 == 0:
            n["messages"].pop()
        nodes.append(n)
    rows = sorted(indexed(tmp_path, nodes), key=priority)
    expected = set()
    for a, b in itertools.combinations(rows, 2):
        if (a["mask_count"] or b["mask_count"]) and possible_pair(a, b):
            expected.add((b["ref"]["id"], a["ref"]["id"]))
    actual = [(r["short"]["id"], r["long"]["id"]) for r in group_candidates(("x", rows))]
    assert set(actual) == expected
    assert len(actual) == len(expected)


def test_composed_mapping_is_rejected_without_direct_raw_witness(tmp_path):
    nodes = [sample(str(i), True) for i in range(3)]
    for n, name in zip(nodes, ("Alice", "[USER]", "Bobby")):
        n["messages"][0]["content"] = name
    refs = [r["ref"] for r in indexed(tmp_path, nodes)]
    assert raw_relaxed_covers(nodes[0], nodes[1], refs[0], refs[1], POLICY)
    assert raw_relaxed_covers(nodes[1], nodes[2], refs[1], refs[2], POLICY)
    row = compose(({"record": refs[0], "retained": refs[1], "status": "removed"},
                   {"record": refs[1], "retained": refs[2], "status": "removed", "model": "test"}))
    assert list(direct_audit([row], StorageConfig()))[0]["passed"] is False


def test_string_bounds_never_reject_a_true_bounded_match():
    rng = random.Random(1030)
    for _ in range(4000):
        a = "".join(rng.choice(["a", "bb", "<PERSON>", "[SENSITIVE]", "x" * 300, "\n"])
                    for _ in range(rng.randint(1, 6)))
        b = "".join(rng.choice(["a", "bb", "<PHONE>", "[USER]", "x" * 300, "\n"])
                    for _ in range(rng.randint(1, 6)))
        if string_matches(a, b):
            assert bounds_possible(string_bound(a), string_bound(b)), (a, b)
    for n in [1, 63, 64, 65, 255, 256]:
        a, b = "leading" * 30 + "[USER]" + "tail" * 30, "leading" * 30 + "x" * n + "tail" * 30
        assert bounds_possible(string_bound(a), string_bound(b))


def test_multiline_bounds_keep_every_independently_verified_match():
    rng = random.Random(171030)
    for _ in range(1500):
        fragments = [rng.choice(["plain", "\r", "\n", "\r\n", "\u2028", "<PERSON>",
                                 "[SENSITIVE]", "<PATH>", "中文"])
                     for _ in range(rng.randint(1, 12))]
        pattern = "".join(fragments)
        value = "".join(rng.choice(["Alice", "x" * 256, "<PHONE>", "中\u2028文"])
                        if s in {"<PERSON>", "[SENSITIVE]"} else s for s in fragments)
        assert string_oracle(pattern, value)
        assert bounds_possible(string_bound(pattern), string_bound(value))
        assert bounds_possible(string_bound(value), string_bound(pattern))
        assert bounds_possible(string_bound(pattern, include_words=True),
                               string_bound(value, include_words=True))
        assert bounds_possible(string_bound(value, include_words=True),
                               string_bound(pattern, include_words=True))


def test_prompt_markers_do_not_hide_different_literal_lines():
    prefix, suffix = "shared prefix " * 10, " shared suffix" * 10
    a = prefix + "\nquestion one\n<REASON>\n" + suffix
    for middle in ("\nquestion two\n<REASON>\n", "\nquestion\none\n<REASON>\n",
                   "\rquestion one\n<REASON>\n"):
        b = prefix + middle + suffix
        assert not string_oracle(a, b)
        assert not bounds_possible(string_bound(a), string_bound(b))
    # Different directions on different lines must not establish a match.
    assert not bounds_possible(string_bound("<PERSON>\nAlice"),
                               string_bound("Bobby\n<PERSON>"))


def test_multiline_candidates_preserve_raw_matches_and_reject_literal_changes(tmp_path):
    texts = ["shared\nhello Alice\n<REASON>\nend",
             "shared\nhello <PERSON>\n<REASON>\nend",
             "shared\nhello Bobby\n<REASON>\nend",
             "shared\nother subject\n<REASON>\nend",
             "shared\r\nhello Alice\n<REASON>\nend"]
    nodes = [sample(str(i), True) for i in range(len(texts))]
    for node, text in zip(nodes, texts):
        node["messages"][0]["content"] = text
    rows = indexed(tmp_path, nodes)
    candidates = {frozenset((p["short"]["id"], p["long"]["id"]))
                  for p in group_candidates(("same-scope", rows))}
    expected = set()
    for i, j in itertools.combinations(range(len(rows)), 2):
        ar, br = rows[i]["ref"], rows[j]["ref"]
        if raw_relaxed_covers(nodes[i], nodes[j], ar, br, POLICY):
            expected.add(frozenset((ar["id"], br["id"])))
    assert candidates == expected
    assert len(expected) == 2


def test_literal_line_index_equals_exhaustive_candidates_with_legacy_and_short_rows(tmp_path):
    rng = random.Random(711086)
    nodes = []
    for i in range(90):
        node = sample(str(i), True)
        node["messages"][0]["content"] = rng.choice([
            "prefix\nAlice\n<REASON>\nend", "prefix\n<PERSON>\n<REASON>\nend",
            "prefix\nBobby\n<REASON>\nend", "prefix\r\nAlice\n<REASON>\nend",
            "\n".join(["literal " + str(j) for j in range(40)] + ["<PERSON>"]),
        ])
        node["messages"][1]["content"] = rng.choice(["", "<PERSON>\nanswer", "Alice\nanswer"])
        if i % 4 == 0:
            node["messages"] = node["messages"][:2]
        nodes.append(node)
    rows = sorted(indexed(tmp_path, nodes), key=priority)
    for i, row in enumerate(rows):
        if i % 3 == 0:
            row["string_bounds"] = tuple({k: v[:9] for k, v in bounds.items()}
                                         for bounds in row["string_bounds"])
    expected = {(b["ref"]["id"], a["ref"]["id"]) for a, b in itertools.combinations(rows, 2)
                if (a["mask_count"] or b["mask_count"]) and possible_pair(a, b)}
    actual = [(r["short"]["id"], r["long"]["id"]) for r in group_candidates(("x", rows))]
    assert set(actual) == expected
    assert len(actual) == len(expected)


def test_word_index_matches_exhaustive_filter_in_both_directions_and_partial_legacy(tmp_path):
    nodes = []
    for text in ("check AlphaProject with <PERSON> before ending",
                 "check AlphaProject with alice before ending",
                 "check BetaProject with alice before ending",
                 "check <PROJECT> with alice before ending",
                 "<MASKED>", "AlphaProject<PERSON>", "AlphaProjectAlice",
                 "<PERSON>AlphaProject", "AliceAlphaProject", "研究中文AlphaProject"):
        node = sample(str(len(nodes)), True)
        node["messages"][0]["content"] = text
        nodes.append(node)
    rows = sorted(indexed(tmp_path, nodes), key=priority)
    # Mix current, fully old and partly old word-index metadata.
    for i, row in enumerate(rows):
        if i % 3 == 0:
            row["string_bounds"] = tuple({k: v[:10] for k, v in item.items()}
                                         for item in row["string_bounds"])
        elif i % 3 == 1:
            row["string_bounds"][1]["content"] = row["string_bounds"][1]["content"][:10]
    expected = {(b["ref"]["id"], a["ref"]["id"]) for a, b in itertools.combinations(rows, 2)
                if (a["mask_count"] or b["mask_count"]) and possible_pair(a, b)}
    actual = [(r["short"]["id"], r["long"]["id"]) for r in group_candidates(("x", rows))]
    assert set(actual) == expected
    assert len(actual) == len(expected)
    for i, j in itertools.combinations(range(len(nodes)), 2):
        a, b = nodes[i]["messages"][0]["content"], nodes[j]["messages"][0]["content"]
        if string_oracle(a, b):
            assert bounds_possible(string_bound(a, include_words=True),
                                   string_bound(b, include_words=True))


def test_word_index_preserves_opposite_directions_between_messages(tmp_path):
    texts = [
        ("AlphaProject handles Alice", "<PROJECT> handles Bobby"),
        ("<PROJECT> handles Alice", "BetaProject handles Bobby"),
        ("GammaProject handles Alice", "DeltaProject handles Bobby"),
        ("GammaProject handles <PERSON>", "DeltaProject handles <PERSON>"),
    ]
    nodes = []
    for i, (first, second) in enumerate(texts):
        node = sample(str(i), True)
        node["messages"][0]["content"] = first
        node["messages"][1]["content"] = second
        nodes.append(node)
    rows = indexed(tmp_path, nodes)
    candidates = {frozenset((r["short"]["id"], r["long"]["id"]))
                  for r in group_candidates(("x", rows))}
    assert raw_relaxed_covers(nodes[0], nodes[1], rows[0]["ref"], rows[1]["ref"], POLICY)
    assert frozenset((rows[0]["ref"]["id"], rows[1]["ref"]["id"])) in candidates
    for i, j in itertools.combinations(range(len(nodes)), 2):
        if raw_relaxed_covers(nodes[i], nodes[j], rows[i]["ref"], rows[j]["ref"], POLICY):
            assert frozenset((rows[i]["ref"]["id"], rows[j]["ref"]["id"])) in candidates
