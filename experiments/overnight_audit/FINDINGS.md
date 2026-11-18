# Independent audit — interim findings, 2026-11-18

This audit explains retained trajectories without changing the production
comparison policy, schema, or published objects. The rollback tag is
`checkpoint-20261118-before-independent-audit` at `951f4e2`.

The three published 2026-10-30 retained sets contain 1,573,632 trajectories:
FreeRouter 565, Boyue 290,893, and TokenPlan 1,282,174. Every object was read and
SHA256-checked. Every diagnostic coverage candidate was checked again against
both complete JSON objects. DeepInfra remains pending; no conclusion here
claims to cover its final retained set.

The later removal-side audit found a separate correctness concern: a literal
`<MASK>` fill-in-the-blank task marker was treated as a redaction wildcard, and
two more published merges have conflicting task evidence in captured reasoning.
The retained-side checks below do not establish that every removal was safe.

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

## Completed local attribution

All 105,073 different directed candidate pairs were reread in full and explained
locally. They involve 104,555 different short records: Boyue 10,775, FreeRouter
2, and TokenPlan 93,778. No candidate was covered by the unchanged production
policy, and its projected and raw verifiers agreed on every pair. This conclusion
applies to the candidates found by these diagnostics, not every conceivable
semantic equivalence.

| Dataset | Primary explanation | Pairs |
|---|---|---:|
| Boyue | Redacted-identity protection | 10,393 |
| Boyue | Leading system/developer messages | 335 |
| Boyue | Empty visible assistant positions | 28 |
| Boyue | Text/argument formatting | 22 |
| Boyue | Runtime notice; mixed differences | 1; 1 |
| FreeRouter | Leading system/developer messages; runtime notice | 1; 1 |
| TokenPlan | Identity boundary | 79,134 |
| TokenPlan | Leading system/developer messages | 6,611 |
| TokenPlan | Assistant leading whitespace only | 5,992 |
| TokenPlan | Runtime notices | 983 |
| TokenPlan | Mixed header/notice/format differences | 832 |
| TokenPlan | Empty visible assistant positions | 397 |
| TokenPlan | Text/argument formatting | 298 |
| TokenPlan | Missing user/assistant exchange | 34 |
| TokenPlan | Unresolved tool binding; tool-name mismatch | 6; 4 |

These are mutually exclusive explanations for pairs. A short record can have
different witnesses and different explanations; pair counts are not removal
counts. In particular, the Boyue redacted-identity bucket involves 10,388
different short records, whereas the stable-message-only bucket was 10,379
pairs. The difference comes from adding the other diagnostic views.

In the 5,992 leading-whitespace pairs, the first difference is at the short
record's final message in 5,985 cases. In 5,782 cases, that first difference is
two leading newlines versus none. Five complete deterministic examples were
read locally: the only stable-message difference in each was two leading
newlines on the final assistant message, and the complete histories aligned
after that change. This is a concrete source of presentation duplicates, not
evidence that every leading space can safely be stripped: indentation may carry
meaning, and the production rule remains unchanged.

One of these five examples was traced to the collection envelopes. The earlier
request has 10 messages and the later request has 12; their first 10 messages
are exactly equal. The earlier captured response begins with two newlines; the
later request replays the assistant text without them and with exactly equal
tool calls. Both standardized files inherit their corresponding captured text
exactly. The requests arrived 8.745 seconds apart, with the later request 1.037
seconds after the earlier response completed. The difference therefore predates
TrajFoundry standardization. The response capture is explicitly
`OPENAI_CHAT_COMPLETIONS_NORMALIZED`, not raw network bytes; the evidence does
not identify whether a client, gateway, or capture normalization changed it.

An additional local pass reread all 5,992 leading-whitespace pairs (7,620
distinct source objects, 2,050,514,679 bytes) and checked every visible message
after the existing canonicalization. Removing only leading CR/LF characters
restores the complete prefix for 5,904 pairs; 5,898 of them differ only in the
last assistant. In 5,811 pairs, each changed position differs by exactly two
newline characters. The remaining 88 pairs require removing a space or tab on
the content line; 87 differ only in the last assistant. Removing complete blank
lines while preserving content-line indentation adds no further pairs.

The 5,904 line-break pairs consist of Sage-A1.5 2,748, Agents-A1 1,250,
qwen3.8-27b 969, intern-s2 933, and deepseek-v4.1-flash-1017 4. Each category's
pair count also equals its distinct short-record count. All source hashes and
complete prefixes were checked, not just the first differing character. Nine
model/whitespace examples from the 88 space/tab pairs were inspected as a
separate boundary. These 5,992 candidates are already part of the 105,073-pair
attribution total and must not be added again. No production rule or source
object was changed. Full evidence is in `leading-whitespace-three-published/`
outside Git; the comparison still ignores the fields excluded by the existing
visible-history policy and does not assert semantic equivalence.

The leading-header sample demonstrates why this bucket is not automatically
removable. Examples include elapsed runtime timestamps, a saved command
approval, changed agent-to-agent roles, changed model identity/response
instructions, different social context, additional battery-design advice, and
retrieved code memories. Some are dynamic annotations; others change the
instructions or evidence supplied to the model.

## Mid-history system/developer probe

Removing every system/developer message was used only as a broad diagnostic.
Of 8,411 pairs outside the stable view, 1,455 were not already covered by the
leading-header view. All 1,455 were reread and attributed locally: 808 TokenPlan
pairs lack a user/assistant exchange and remain protected; 644 TokenPlan and
3 Boyue pairs have system/developer differences inside an otherwise aligned
history, without those eligibility protections.

Among the 644 TokenPlan pairs, the descriptive content categories are 436 other
system/developer content, 153 token-budget plus planning hints, 20 token-budget
only, 6 Git Bash notices, 1 planning hint, and 28 mixed categories. The 3 Boyue
pairs include 2 with complete cached Read-file evidence in system messages and
1 with an agent catalog plus other instructions. Ignoring these messages could
discard real file content or change the task context. The 647 pairs are review
candidates, not authorized deletions, and are not additive to the other views.

## Original collection evidence for changed user IDs

One cross-user TokenPlan example was traced to both original masked-raw
requests. Their message counts are 30 and 32, with an exact full prefix of all
30 earlier request messages. Receipt times differ by 5.954 seconds; the second
request arrived 1.07 seconds after the earlier response completed. The raw
envelopes already have different `source_user_id` values, and each normalized
`metadata.user_id` exactly inherits its own raw value.

The request bodies supply no user/session metadata, and neither record has a
known session or end-user identity. This is strong evidence of a related
workflow, but it does not establish the real user or the reason the collector's
source user ID changed. The standardizer did not introduce the ID difference.
Raw requests, source hashes, and comparisons remain outside Git.

## Harness-boundary check

A further local scan checked whether a change in harness labels could split
otherwise identical model/user/session scopes. All 1,573,632 indexed records
were counted. After removing only harness from the identity key, no group
contained more than one harness label, so this dataset has zero candidates
arising solely from this boundary. This does not test changing model or other
identity fields simultaneously.

The experimental scan supports one verified other-harness witness for every
covered record, including cases hidden by same-harness sorted neighbors. Its
completeness check matches an exhaustive search over randomized synthetic
cases. Actual execution was `local[8]`, exit 0, with no S3 writes.

## Thought-only responses and replay layout

All 425 pairs primarily attributed to empty-visible-assistant positions were
read again locally: TokenPlan 397 and Boyue 28. These counts are subsets of the
earlier attribution, not new additions. The earlier empty probe's 434 incremental
pairs also included 9 Boyue pairs subsequently explained by identity protection.

| First blocking difference | TokenPlan | Boyue |
|---|---:|---:|
| Same reasoning split from an action into a separate assistant | 87 | 0 |
| Same complete reasoning text embedded in visible assistant content | 30 | 0 |
| Terminal thought-only response differs | 239 | 21 |
| Terminal fully empty response differs | 2 | 7 |
| Other internal empty-assistant layout differences | 39 | 0 |

For all 87 split cases, the reasoning text is exactly equal, the following
visible action matches, and removing only the additional empty-visible position
restores a complete visible prefix. The other categories include incomplete
responses, repeated attempts, and an empty image-task response versus one with
an image. They cannot all be called duplicate successful trajectories.

Eight selected pairs were manually read. Two were traced to both collection
envelopes, preserving the distinction between captured normalized responses and
original network bytes:

- A Responses API request grows from 41 to 45 input items, with all earlier
  items unchanged. A 547-character `reasoning_text` output is replayed in a
  later assistant `output_text`, surrounded by `<thinking>` tags, followed by
  an idle-turn reminder. The standardized objects inherit these representations.
- A Chat Completions request grows from 406 to 410 messages, with all earlier
  messages unchanged. Its previous response is one assistant with 3,342
  reasoning characters and two tool calls. The next request has a thought-only
  assistant followed by a tool-call assistant. Reasoning text, call IDs, names,
  and arguments are exactly equal. The split already exists before TrajFoundry.

The production rule ignores reasoning fields but deliberately retains the
positions of thought-only messages. It therefore does not automatically handle
this change in message layout or reasoning replayed as visible content. These
findings explain retention without adding a rule. Full JSON, receipts, and manual
judgments remain outside Git.

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

## Removal-side check: task placeholders and masked semantic conflicts

The 11,324 additional masking removals include 56 equal-length pairs: 38 Boyue
and 18 TokenPlan. All 56 pairs were reread locally with their recorded hashes,
together with 10 additional extension pairs chosen for different fields and
labels. This is 66 complete pairs, not a second full scan of all removals.
Twelve selected pairs were manually reviewed at their differences and task
context, then two more equal-length cases were followed up. Nine selected
extension pairs are consistent with masked replay; two selected pairs remain
ambiguous. These targeted samples do not estimate a population error rate.

Three TokenPlan pairs were traced through both masked-raw collection envelopes,
the original normalized objects, and the actual v010 publication:

1. **Task placeholder classified as redaction.** A climate-science question has
   three literal `<MASK>` blanks in one request, versus one blank and two
   explicit `aerosol` words in the other. Both responses answer `aerosol`.
   Both captured reasoning texts also identify the different blank counts.
   The literal task markers are part of the question; this is a confirmed
   example of the masking comparison ignoring a real task-format difference.
2. **Conflicting title targets.** One output is wholly `<ADDRESS>`, while its
   captured reasoning repeatedly describes and counts characters for a title
   about one town. The retained request and answer concern a different town.
   This is strong evidence of a semantic mismatch, but the first unmasked
   request cannot be reconstructed from the available masked capture.
3. **Different inputs with the same empty extraction result.** Both outputs
   are `[]`. One captured reasoning text analyzes a remark about an Internet
   addiction center; the other analyzes a different remark about prior advice.
   A broad `<ORGANIZATION>` span covers the difference in the first request.
   Equal extractor outputs do not establish equal task inputs. This is another
   strong semantic-conflict example, subject to the same masked-input limit.

All six request bodies match their respective normalized histories exactly.
The response role, visible content and reasoning also match exactly; the
climate pair's normalized form omits an empty `tool_calls` field. Capture formats
are `ORIGINAL`, but the objects were read from masked-raw: that does not mean
they contain pre-masking network bytes.

For each pair, the successful publication manifest lists only the retained
trajectory, its currently downloaded v010 object matches that manifest's SHA256,
and one published lineage row contains both raw request sources. The full
manifest has 1,282,175 file entries: 1,282,174 trajectories plus `lineage.jsonl`.
A GET on each removed path returned AccessDenied; this was not treated as proof
of absence. Publication membership is established by the complete manifest and
lineage instead.

This evidence adds one confirmed non-redaction task-marker match and two strong
semantic-conflict cases; it does not justify labeling all 56 equal-length pairs
incorrect. The existing rule deliberately ignores reasoning, so these findings
also illustrate a limitation of the comparison policy, not an implementation
disagreement. No production rule, running job, or published object was changed.
Private source objects, receipts, judgments and publication proofs are in
`manual-mask-removals/` under the independent audit run, outside Git.

A follow-up completed the entire short-record boundary: all 72 masking removals
with at most six messages on the removed side (Boyue 41, TokenPlan 31). The
previous check already covered all 55 equal-length pairs in this boundary;
the remaining 17 extension pairs were read and hash-checked locally. Across
both runs, 82 distinct complete pairs have been read; one extension example
was intentionally present in both selections.

For all 17 short extension pairs, the raw messages before the final assistant
are exactly equal, the final tool-call IDs are exactly equal, and every visible
difference is in that final assistant's content or arguments. All 17 contain
tools. Three also have reasoning differences: one replaces a place name with
a location marker; two omit reasoning in the replay. Inspection found no new
explicit task conflict in these extension pairs. This is stronger replay
evidence than matching a shared template and an empty classifier result, but
does not reconstruct the hidden pre-masking values or establish an error rate
for all 11,324 masking removals.

The longer-history follow-up read 30 pairs from the 11,252 removals whose short
side has more than six messages: 10 Boyue, 8 FreeRouter, and 12 TokenPlan. The
selection covers length/session strata and deliberately chosen boundaries such
as the earliest changed user message, greatest field-length imbalance, and
largest number of changed fields. All 57 distinct source objects were read
completely and checked against their byte lengths and SHA256 hashes.

Twenty-eight pairs have exactly corresponding original tool-call ID sequences;
two also involve ID rewriting. Fourteen pairs have visible differences only in
the final assistant; sixteen differ at earlier message positions as well.
Manual review covered visible field differences, task context, and selected
surrounding messages. It did not add a confirmed task conflict. Examples include
user-role timezone snapshots, repeated chat identity markers, a real travel
question with an otherwise identical long history, and numerical output falsely
labeled as a phone number. Long common history and matching operations support
replay in these cases, but do not reveal the hidden values.

Nine pairs also differ in reasoning fields: three only in ancillary metadata,
four in masking of text, and two by omitted reasoning. No additional conflicting
task interpretation was found in this review. Two pairs overlap the preceding
82-pair evidence set, so the combined removal-side evidence now contains 110
distinct pairs. This targeted check neither estimates an overall error rate nor
invalidates the confirmed short-task placeholder issue and two strong semantic
conflicts above. Private evidence is in `long-mask-boundaries/` outside Git.

## Early DeepInfra candidate check, before completion

The original job is still generating candidates. A read-only capture froze
313,589,760 bytes of its active partition's temporary output. The immutable
prefix contains 267,016 complete candidate rows, all in the previously identified
large group, referencing 34,497 distinct short IDs and 36,339 distinct long IDs.
Its final 1,060 bytes are an incomplete JSON row and are excluded. These are
partial candidate counts, not completed matches, removals, or publication counts.

Eleven pairs were selected at sequence positions and file-size boundaries;
all 22 complete original normalized objects were read and hash-checked. The
model is Atria-Dawn throughout. Eight are longer-history examples with masking
differences near the last assistant. Three are three-message command-review
requests with equal answers but clearly different commands.

The unchanged code bundled with `control-refine-02` was imported locally.
Both its next-stage pair matcher and its independent raw-object oracle accept
the eight longer-history examples and reject all three different-command pairs.
This is a useful boundary check: appearing in a coarse candidate file does not
mean the pair passes comparison, and passing comparison does not yet establish
the final representative or publication decision. The broad marker recognizer
also lists literal severity labels `[HIGH]` and `[MEDIUM]` as redaction markers;
the command differences still reject the three observed pairs. No actual
mismerge is established by these candidate examples.

The frozen byte prefix, complete source JSON, manual notes, code hashes and both
comparison receipts are in `deepinfra-partial-candidates/` outside Git. The
reproduction script uses only those existing local files. No trajectory command
was executed, no new Cluster job was submitted, and the original run and core
code remain unchanged. Final DeepInfra retained-set analysis is still pending.

## Complete user-message masking subdivision

Among the three completed channels' 11,324 extra masking removals, 7,613 pairs
have changed user messages. All 7,613 were read locally, covering 9,862 distinct
complete source objects and 5,833,910,693 bytes with length and SHA256 checks.
The initial narrow comparison finds 6,742 pairs whose user changes consist only
of the exact `[Asia/<LOCATION>]` versus `[Asia/Shanghai]` spelling.

The remaining 871 pairs were reread entirely from the local cache. A broader
location-only diagnostic distinguishes timezone names in `Asia/...`, sender-ID
slots, and bot-identity slots, requiring the full surrounding user text to agree.
It rejects two different concrete values and rejects unrelated body differences.
The resulting mutually exclusive pair counts across all 7,613 are:

| User-text difference location | TokenPlan | FreeRouter | Boyue |
| --- | ---: | ---: | ---: |
| Timezone slots only | 6,864 | 1 | 0 |
| Sender-ID slots only | 165 | 0 | 0 |
| Bot-identity slots only | 142 | 0 | 0 |
| Includes other user text | 403 | 0 | 38 |

These categories concern user messages only: assistant or tool fields may also
differ. They do not reconstruct hidden values or establish the semantic
correctness of the removals. The 441 other-text pairs include the previously
documented task-placeholder counterexample and strong suspected conflicts;
441 is not a count of errors. Of those 441, 305 have exactly corresponding
original tool-call ID prefixes, providing additional replay evidence without
proving hidden inputs equal.

Eight additional, nonoverlapping boundary pairs were inspected through complete
visible-field differences and surrounding context. Examples include a model
route name masked as a place, the Chinese hair-description word masked as a
person, a memory-instruction term masked as an organization, and test strings
masked as IP addresses. One long coding history includes a real user question
with a masked place name, reinforcing that not all user changes are metadata.
No new confirmed task conflict was established by these eight samples. They
are deliberately selected examples, not a population error-rate sample.

Private reports, pair references, full cached sources and manual packets are in
`masked-user-changes-three-published/` and `masked-user-change-locations/` under
the independent audit run. Source trajectories remain outside Git. Production
rules, core code, the original DeepInfra job and published data are unchanged.

## Execution constraints and final-channel work

The user revised the objective to require local experiments. The completed
Cluster diagnostic results remain usable. Pending experiment submitters were
stopped, and the in-progress attribution experiment Driver was deleted using
its exact UID. Further attribution and structure probes use `local[8]` on the
current host; no more experimental Cluster jobs will be submitted.

The original DeepInfra production run continues unchanged. Once its validated
publication completes, the existing delivery process will fill the Feishu
comparison, and the retained-set diagnostics will run locally. The final audit
must still include these results and resolve the outstanding candidate counts.
