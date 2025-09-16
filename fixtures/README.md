[BRO NOTE] Submission Derivation Fixture

Purpose
- Tiny, safe sample to sanity-check `helpers/derivations.py::derive_submission`.
- No DAP needed. Run locally to verify classification and lateness logic.

Files
- `submission_derivation_sample.csv`: 10 rows covering common cases.

How to use
1) `python tools/check_derivation.py fixtures/submission_derivation_sample.csv`
2) Inspect printed counts by `classification_source` and a preview of derived columns.
3) Tweak `helpers/derivations.py` if something feels off. Prefer `unknown` over guessing.

Columns used (best effort)
- id, assignment_id, course_id, user_id (IDs as strings)
- submitted_at, graded_at, workflow_state, submission_type, due_at
- attempt, attachments, no_submission (optional signals)

