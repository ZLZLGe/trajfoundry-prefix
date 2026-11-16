from copy import deepcopy

from trajectory_prefix.projection import Policy, covers, project


def sample(header):
    return {
        "metadata": {"session_id": "s"},
        "model": "m",
        "messages": [
            {"role": "system", "content": header},
            {"role": "user", "content": "Please inspect the project"},
            {"role": "assistant", "content": "Inspecting", "reasoning_content": ""},
        ],
    }


def test_only_leading_cch_changes_are_ignored():
    a = sample("x-anthropic-billing-header: cc_version=1.2; cch=c567c;\nReal instructions")
    b = deepcopy(a)
    b["messages"][0]["content"] = a["messages"][0]["content"].replace("c567c", "6f2ad")
    before = deepcopy([a, b])
    assert not covers(project(a, "x", "d", Policy(())), project(b, "x", "d", Policy(())))
    assert covers(project(a, "x", "d"), project(b, "x", "d"))
    assert [a, b] == before
    b["messages"][0]["content"] += " different instruction"
    assert not covers(project(a, "x", "d"), project(b, "x", "d"))


def test_user_text_and_version_are_not_masked():
    a = sample("x-anthropic-billing-header: cc_version=1.2; cch=123;\nInstructions")
    b = deepcopy(a)
    b["messages"][0]["content"] = b["messages"][0]["content"].replace("1.2", "1.3")
    assert not covers(project(a, "x", "d"), project(b, "x", "d"))
    a["messages"][0]["role"] = "user"
    b = deepcopy(a)
    b["messages"][0]["content"] = b["messages"][0]["content"].replace("123", "456")
    assert not covers(project(a, "x", "d"), project(b, "x", "d"))
