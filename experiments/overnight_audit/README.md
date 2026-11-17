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
