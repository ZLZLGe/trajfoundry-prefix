# Independent audit — interim findings, 2026-11-18

This audit explains retained trajectories without changing the production
comparison policy, schema, or published objects. The rollback tag is
`checkpoint-20261118-before-independent-audit` at `951f4e2`.

The three published 2026-10-30 retained sets contain 1,573,632 trajectories:
FreeRouter 565, Boyue 290,893, and TokenPlan 1,282,174. Every object was read and
SHA256-checked. Every diagnostic coverage candidate was checked again against
both complete JSON objects. DeepInfra remains pending; no conclusion here
claims to cover its final retained set.

## Existing-policy candidates

The independent stable-message view found no FreeRouter coverage candidates,
10,379 Boyue pairs, and 37 TokenPlan pairs. All Boyue pairs contain the existing
redacted-identity protection issue. All 37 TokenPlan pairs were separately read
and explained locally: 27 lack a user/assistant exchange, 6 have an unresolved
tool-result binding, and 4 have a tool-name binding mismatch. None of these
37 pairs is covered by the unchanged production policy.

These results explain the candidates in this diagnostic view. They do not
establish that every semantically repetitive history has been removed.

The cross-identity view found 79,134 TokenPlan pairs whose identity differs:
79,041 differ only in user ID. Of the 79,134 pairs, 78,701 have no session ID.
Identical long message history does not independently establish why user IDs
changed or whether the same real person/session generated both objects.

The empty-visible-assistant probe found 434 additional candidate pairs (397
TokenPlan, 37 Boyue). These include reasoning-only responses omitted on replay,
but also other empty-response cases. The initial report's reasoning-only
subcounts used literal empty content and can miss whitespace-only content;
the local attribution report adds counts after the existing visible trimming.
The 434 candidate count is unaffected by that reporting distinction.

Counts from different diagnostic views overlap and are not additive removal
counts. Local attribution deduplicates directed pairs and counts distinct short
records while separating eligibility protections from content differences.

## Manual branch review

Thirty-six pairs were read, twelve per channel, in addition to the earlier three
FreeRouter high-sub-number pairs. Selection intentionally covers different
divergence roles and long common histories; these are not population estimates.
The reviewer read task context and messages around the divergence, and checked
complete message alignment where a presentation difference was suspected.

| Manual assessment | Pairs |
|---|---:|
| Auxiliary requests: summaries, recap, suggested input, review, memory | 14 |
| History presentation/replay differences | 8 |
| Tool-call semantic-equivalence candidates | 2 |
| Retries or incomplete responses | 4 |
| Substantive response/execution differences | 3 |
| Mixed differences requiring further evidence | 5 |

Concrete findings include:

- An Edit call explicitly supplies `replace_all: false`; its counterpart omits
  the key. Both tool definitions specify `default: false` and do not require
  that key. This particular comparison has evidence of equivalent arguments.
- A tool result retains the same first 4,096 and last 1,024 characters but
  replaces its middle with `[... tool result middle pruned ...]`. The longer
  conversation can therefore contain a less complete earlier tool result.
- A `The Bash tool shell is Git Bash.` system message inserted in the middle
  of a long history can interrupt an otherwise complete visible prefix.
- Identical runtime token-budget text can appear under `system` or `user`.
- An initially benign-looking system-message difference can be followed by
  different commands, different tool argument names, or a memory/summary
  request. Fixing only the first mismatch does not prove complete coverage.
- A large `_sub_N` can reflect helper calls, compressed contexts, or fixed
  session IDs reused for different tasks; it is not a count of true task forks.

No new production merge is authorized by these observations. In particular,
ignoring all system/developer messages is an intentionally broad local probe,
not a proposed production policy.

Complete private source JSON, hashes, per-pair judgments, and excerpt packets
are retained outside Git in the audit run and in the user's delivery folder.
They are not committed to this repository.

## Execution and remaining work

The user revised the objective to require local experiments. The completed
Cluster diagnostic results remain usable. Pending experiment submitters were
stopped, and the in-progress attribution experiment Driver was deleted using
its exact UID. Further attribution and structure probes use `local[8]` on the
current host; no more experimental Cluster jobs will be submitted.

The original DeepInfra production run continues unchanged. Once its validated
publication completes, the existing delivery process will fill the Feishu
comparison, and the retained-set diagnostics will run locally. The final audit
must still include these results and resolve the outstanding candidate counts.
