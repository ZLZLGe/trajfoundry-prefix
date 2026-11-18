# Independent retained-prefix and branch audit

This worktree starts at `checkpoint-20261118-before-independent-audit`.
Production core files and approved matching policy are unchanged.

After the user's execution constraint changed on 2026-11-18, all further
experiments run on the current host using `local[8]` or local Python threads.
Previously completed Cluster evidence is retained. Pending experimental Cluster
submitters were stopped; the original production DeepInfra job continues.
Input is the complete retained set of the 2026-10-30 publication, read back from
the published version with frozen byte lengths and SHA256. Intermediate evidence
stays on shared/local disk. No S3 mutation or automatic merge is implemented here.

The additional `review_mask_removals_local.py` reads the original normalized
objects referenced by completed masking removals. It collects all equal-length
pairs and a deterministic selection of extension pairs, checks complete source
hashes, and exposes differences without the masking matcher. Its `passed` flag
means the evidence was collected successfully, not that the removals are
semantically correct. Manual findings and raw-source follow-ups are separate.
The `--only-short-extensions` option completes the boundary with every removed
record of at most six messages whose retained witness is longer. Its report
distinguishes the source population from the equal-length pairs actually read.
The mutually exclusive `--long-boundary-sample` option selects records with more
than six messages by channel, length/session strata, and visible-difference
boundaries. Selection is deterministic; it is not an error-rate sample.

`explain_leading_whitespace_local.py` rereads the already attributed
leading-whitespace candidates. It compares complete visible histories after
removing only leading line breaks, only complete blank lines, or general leading
ASCII whitespace. The first content line's indentation survives the blank-line
view. This refines the diagnostic count and does not alter production matching.

`review_partial_candidates_local.py` reproduces a deliberately separate check
of an immutable prefix captured from the unfinished DeepInfra candidate file.
It requires the snapshot and all selected source objects to exist locally,
checks their hashes, and loads the unchanged original job's comparison code.
Candidate counts and local pair checks are never reported as final removals.

`explain_masked_user_changes_local.py` reads every completed masking removal
whose user messages differ. It verifies complete source hashes and isolates
the exact `[Asia/<LOCATION>]` versus `[Asia/Shanghai]` spelling difference.
`subdivide_masked_user_changes_local.py` then reads only the remaining cached
pairs, with no network access, and locates timezone, sender-ID and bot-identity
slots. A visible value cannot match a different visible value in these views;
the difference must involve an existing mask. These are descriptions of where
user text changed, not permission to merge or a semantic correctness verdict.

`review_completed_mask_literal_local.py` reproduces the observed DeepInfra
task-placeholder merge using already captured local objects, completed decisions,
and the original job's frozen comparison code. It checks the result with and
without treating `<MASK>` as a redaction, without changing production. It also
checks that the full messages match the earlier TokenPlan example and keeps the
completed-decision evidence distinct from final publication membership.

The diagnostic views probe text formatting, serialized arguments, leading headers,
runtime notices, mask label differences, identity boundaries, and adjacent branch
histories. A transformed prefix is only a review candidate. In particular, removing
instructions or runtime notices may erase real task differences. Full raw JSON
comparison verifies every reported coverage relation; human reading determines
whether selected branch pairs are actual divergent continuations.

Candidate counts are per view and can overlap. Branch neighbors are a systematic
candidate sample, not an exhaustive classification of every branch. Exact input
coverage, per-mode verified counts, sample selection and limitations are recorded
in the resulting report.
