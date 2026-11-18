"""Comparison-only projections. Never use a projection as an output trajectory."""

from __future__ import annotations

import hashlib
import re
from collections import Counter
from dataclasses import dataclass
from typing import Any

from .comparison import PREFIX_RULES, validate_rules
from .json_codec import canonical as canonical


def digest(value: bytes) -> bytes:
    return hashlib.sha256(value).digest()


@dataclass(frozen=True)
class Policy:
    rules: tuple[str, ...] = PREFIX_RULES
    anonymous_min_messages: int = 6
    anonymous_min_chars: int = 1024
    extra_information: str = "tools_only"

    def __post_init__(self):
        validate_rules(self.rules)
        if self.extra_information not in {"messages_only", "tools_only", "preserve_all"}:
            raise ValueError("Unknown extra-information policy")


@dataclass(frozen=True)
class Projection:
    scope: bytes
    tokens: tuple[bytes, ...]
    vector: bytes
    guard: bytes
    assets: dict[str, tuple[bytes, ...]]
    changes: tuple[str, ...]
    mergeable: bool
    isolation_reason: str


DERIVED = {
    "total_rounds",
    "total_tool_calls",
    "total_tokens",
    "reasoning_total_tokens",
    "tool_counts",
    "completeness",
    "completeness_tag",
    "tool_defs_tag",
    "missing_tool_defs",
    "tool_call_tag",
    "tool_call_check",
    "normalization_audit",
    "termination",
}
PROVENANCE = {"source", "metadata"}
ASSETS = {"tools", "multimodal_file_mapping", "server_tool_calls", "agent_messages"}
METADATA_PROVENANCE = {
    "source_file",
    "source_name",
    "source_type",
    "specific_source",
    "line_no",
    "created_at",
    "model",
    "user_id",
    "end_user_id",
    "session_id",
    "sessionless_chunk_id",
    "sub_session_id",
}
SENTINELS = {"", "no_session_id", "no_user_id", "null", "none", "unknown"}
BILLING_CCH = re.compile(r"\A(x-anthropic-billing-header:[^\n]*?\bcch=)[0-9a-fA-F]+(?=;)")
CLAUDE_PREAMBLE = "You are Claude Code, Anthropic's official CLI for Claude."
PREAMBLE_BILLING_LINE = re.compile(
    r"x-anthropic-billing-header: cc_version=[0-9][A-Za-z0-9._+-]*; "
    r"cc_entrypoint=[A-Za-z0-9_-]+; cch=[0-9a-fA-F]+;[ \t\r]*"
)


def normalize_header(text: str, policy: Policy, changes: set[str]) -> str:
    if "billing_cch" in policy.rules:
        normalized = BILLING_CCH.sub(r"\g<1><volatile-cch>", text)
        if normalized != text:
            changes.add("billing_cch")
        text = normalized
    if "claude_preamble_cch" in policy.rules and text.startswith(CLAUDE_PREAMBLE):
        preamble, separator, tail = text.partition("\n\n")
        first_line = tail.partition("\n")[0]
        if (
            separator
            and "```" not in preamble
            and "~~~" not in preamble
            and "x-anthropic-billing-header:" not in preamble
            and PREAMBLE_BILLING_LINE.fullmatch(first_line)
        ):
            normalized = BILLING_CCH.sub(r"\g<1><volatile-cch>", tail)
            if normalized != tail:
                changes.add("claude_preamble_cch")
            text = preamble + separator + normalized
    return text


def identity(value: Any) -> str:
    if not isinstance(value, str) or value.strip().lower() in SENTINELS:
        return ""
    if value.startswith("<") and value.endswith(">"):
        return ""
    return value


def project(node: dict[str, Any], dataset: str, date: str, policy: Policy = Policy()) -> Projection:
    if not isinstance(node, dict) or not isinstance(node.get("messages"), list):
        raise ValueError("Expected a normalized object with a messages array")
    md = node.get("metadata") or {}
    if not isinstance(md, dict):
        raise ValueError("metadata must be an object")
    session, user, end_user = (
        identity(md.get(k)) for k in ("session_id", "user_id", "end_user_id")
    )
    scope = [dataset, node.get("model", ""), node.get("harness", ""), user, end_user]
    scope += ["session", session] if session else ["sessionless", date]
    tokens = []
    changes: set[str] = set()
    roles: Counter[str] = Counter()
    meaningful_chars = 0
    isolation = ""
    bindings: dict[str, tuple[int, str]] = {}
    call_position = 0
    seen_visible_assistant = False
    for message in node["messages"]:
        if not isinstance(message, dict) or not isinstance(message.get("content"), str):
            raise ValueError("Expected normalized messages with string content")
        role = message.get("role")
        if role not in {"system", "developer", "user", "assistant", "tool"}:
            raise ValueError("Unsupported normalized message role")
        roles[role] += 1
        if role in {"user", "assistant", "tool"}:
            meaningful_chars += len(message["content"])
        # Same baseline as the archive normalizer: response reasoning envelopes
        # are not stable across replay. All other message fields are compared.
        value = {
            k: v
            for k, v in message.items()
            if k not in {"reasoning_content", "reasoning_details", "reasoning"}
        }
        if role in {"system", "developer"}:
            value["content"] = normalize_header(value["content"], policy, changes)
        if role == "assistant" and "assistant_trailing_space" in policy.rules:
            trimmed = value["content"].rstrip(" \t\r\n")
            if trimmed != value["content"]:
                changes.add("assistant_trailing_space")
            value["content"] = trimmed
        if role == "assistant" and not value.get("tool_calls"):
            value.pop("tool_calls", None)
        if role == "assistant":
            if value["content"] or value.get("tool_calls"):
                seen_visible_assistant = True
            elif (
                "empty_assistant" in policy.rules
                and seen_visible_assistant
                and set(message)
                <= {
                    "role",
                    "content",
                    "tool_calls",
                    "reasoning_content",
                    "reasoning_details",
                    "reasoning",
                }
                and not any(
                    message.get(k) for k in ("reasoning_content", "reasoning_details", "reasoning")
                )
            ):
                # Replay may omit an entirely empty assistant envelope. Keep
                # leading empty responses, reasoning-only turns, media and
                # unknown extensions. Only the comparison sequence changes.
                changes.add("empty_assistant")
                continue
        if "tool_id_alpha" in policy.rules:
            if role == "assistant" and value.get("tool_calls"):
                calls = []
                for call in value["tool_calls"]:
                    identifier = call.get("id")
                    if not isinstance(identifier, str) or not identifier or identifier in bindings:
                        isolation = "ambiguous_tool_binding"
                    else:
                        bindings[identifier] = (
                            call_position,
                            call.get("function", {}).get("name", ""),
                        )
                    calls.append(dict(call, id=["call-position", call_position]))
                    call_position += 1
                value["tool_calls"] = calls
                changes.add("tool_id_alpha")
            elif role == "tool":
                identifier = value.get("tool_call_id")
                if identifier not in bindings:
                    isolation = "unresolved_tool_binding"
                else:
                    position, name = bindings[identifier]
                    if value.get("name", "") != name:
                        isolation = "tool_name_binding_mismatch"
                    value["tool_call_id"] = ["call-position", position]
                    changes.add("tool_id_alpha")
        tokens.append(canonical(value))
    if not roles["user"] or not roles["assistant"]:
        isolation = "no_user_assistant_exchange"
    # Unknown identities require substantial identical visible history. This is
    # content reuse for classification, not proof of an original session ID.
    if not session and not (user or end_user):
        if (
            len(node["messages"]) < policy.anonymous_min_messages
            or meaningful_chars < policy.anonymous_min_chars
            or not roles["tool"]
        ):
            isolation = "anonymous_insufficient_history"
    audit = node.get("normalization_audit") or {}
    for issue in audit.get("issues", []):
        if issue.get("code") in {"metadata_session_id_masked", "metadata_user_id_masked"}:
            isolation = "redacted_identity"
    guard = {}
    if policy.extra_information == "preserve_all":
        guard = {
            k: v for k, v in node.items() if k not in DERIVED | PROVENANCE | ASSETS | {"messages"}
        }
        if isinstance(guard.get("instructions"), str):
            guard["instructions"] = normalize_header(guard["instructions"], policy, changes)
        guard["metadata_extra"] = {k: v for k, v in md.items() if k not in METADATA_PROVENANCE}
        guard["is_subagent"] = bool((node.get("completeness") or {}).get("is_subagent"))
    assets = {}
    fields = (
        ASSETS if policy.extra_information == "preserve_all"
        else {"tools"} if policy.extra_information == "tools_only"
        else set()
    )
    for field in fields:
        items = node.get(field) or []
        if not isinstance(items, list):
            raise ValueError(f"{field} must be an array")
        assets[field] = tuple(canonical(item) for item in items)
    return Projection(
        canonical(scope),
        tuple(tokens),
        b"".join(digest(t) for t in tokens),
        canonical(guard),
        assets,
        tuple(sorted(changes)),
        not isolation,
        isolation,
    )


def covers(short: Projection, long: Projection) -> bool:
    """Require exact projected prefix bytes and the selected auxiliary checks."""
    if not short.mergeable or not long.mergeable or short.scope != long.scope:
        return False
    if not short.tokens or len(short.tokens) > len(long.tokens) or short.guard != long.guard:
        return False
    if short.tokens != long.tokens[: len(short.tokens)]:
        return False
    return all(
        not (Counter(values) - Counter(long.assets[field]))
        for field, values in short.assets.items()
    )
