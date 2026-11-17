"""Independent bounded matcher and original-object witness verification."""

from bisect import bisect_left

from .masking import MAX_SPAN, markers
from .verification import _eligible, _message, _scope, encoded


def pattern_oracle(pattern, value, limit=MAX_SPAN):
    found = markers(pattern)
    if not found:
        return pattern == value
    first = pattern[:found[0].start()]
    if not value.startswith(first):
        return False
    reachable = [len(first)]
    for index, match in enumerate(found):
        stop = found[index + 1].start() if index + 1 < len(found) else len(pattern)
        literal = pattern[match.end():stop]
        low = reachable[0] + 1
        high = min(len(value), reachable[-1] + limit)
        if index + 1 == len(found):
            positions = [len(value) - len(literal)] if value.endswith(literal) else []
        elif literal:
            positions = []
            pos = value.find(literal, low, high + len(literal))
            while pos >= 0:
                positions.append(pos)
                pos = value.find(literal, pos + 1, high + len(literal))
        else:
            positions = range(low, high + 1)
        following = []
        # Find a preceding reachable endpoint within the character budget and
        # the same line. This oracle does not use the production interval DP.
        for pos in positions:
            if not low <= pos <= high:
                continue
            last_break = max(value.rfind("\r", max(0, pos - limit), pos),
                             value.rfind("\n", max(0, pos - limit), pos))
            candidate = bisect_left(reachable, max(pos - limit, last_break + 1))
            if candidate < len(reachable) and reachable[candidate] < pos:
                following.append(pos + len(literal))
        if not following:
            return False
        reachable = following
    return len(value) in reachable


def string_oracle(a, b, limit=MAX_SPAN):
    return a == b or pattern_oracle(a, b, limit) or pattern_oracle(b, a, limit)


def value_oracle(a, b):
    if type(a) is not type(b):
        return False
    if isinstance(a, str):
        return string_oracle(a, b)
    if isinstance(a, dict):
        return a.keys() == b.keys() and all(value_oracle(a[k], b[k]) for k in a)
    if isinstance(a, list):
        return len(a) == len(b) and all(value_oracle(x, y) for x, y in zip(a, b))
    return encoded(a) == encoded(b)


def _messages(node, policy):
    visible = False
    result = []
    for original in node["messages"]:
        item = _message(original, policy)
        if item["role"] == "assistant" and (item["content"] or item.get("tool_calls")):
            visible = True
        if ("empty_assistant" in policy.rules and visible
                and item == {"role": "assistant", "content": ""}
                and set(original) <= {"role", "content", "tool_calls", "reasoning_content",
                                      "reasoning_details", "reasoning"}
                and not any(original.get(k) for k in
                            ("reasoning_content", "reasoning_details", "reasoning"))):
            continue
        result.append(item)
    return result


def raw_relaxed_covers(a, b, ar, br, policy):
    if policy.extra_information != "messages_only":
        raise ValueError("Relaxed verification requires messages_only")
    if _scope(a, ar["dataset"], ar["date"]) != _scope(b, br["dataset"], br["date"]):
        return False
    if not _eligible(a, policy) or not _eligible(b, policy):
        return False
    aa, bb = _messages(a, policy), _messages(b, policy)
    if not aa or len(aa) > len(bb):
        return False
    bindings = {}
    for x, y in zip(aa, bb):
        if x["role"] != y["role"] or not string_oracle(x["content"], y["content"]):
            return False
        x, y = dict(x), dict(y)
        x.pop("content")
        y.pop("content")
        if x["role"] == "assistant":
            xc, yc = x.pop("tool_calls", []), y.pop("tool_calls", [])
            if len(xc) != len(yc):
                return False
            for left, right in zip(xc, yc):
                if "tool_id_alpha" in policy.rules:
                    bindings[left["id"]] = right["id"]
                    left, right = dict(left), dict(right)
                    left.pop("id")
                    right.pop("id")
                else:
                    left, right = dict(left), dict(right)
                lf, rf = dict(left.pop("function")), dict(right.pop("function"))
                if ("arguments" in lf) != ("arguments" in rf):
                    return False
                if "arguments" in lf and not value_oracle(lf.pop("arguments"), rf.pop("arguments")):
                    return False
                if encoded(left) != encoded(right) or encoded(lf) != encoded(rf):
                    return False
        elif x["role"] == "tool" and "tool_id_alpha" in policy.rules:
            if bindings.get(x.pop("tool_call_id")) != y.pop("tool_call_id"):
                return False
        if encoded(x) != encoded(y):
            return False
    return True
