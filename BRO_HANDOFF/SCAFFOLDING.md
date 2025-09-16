[BRO NOTE] Scaffolding / Quick Start

Key Files
- `helpers/derivations.py` — your `derive_submission(row)` rules (you own this).
- `matt_mini_test.py` — applies `derive_submission` to filtered submissions and writes extra CSV columns.
- `fixtures/submission_derivation_sample.csv` — 10-row sanity sample.
- `tools/check_derivation.py` — quick checker; prints classification counts + preview.

Run Fast
1) `python tools/check_derivation.py fixtures/submission_derivation_sample.csv`
2) `python matt_mini_test.py` (or your normal flow)
3) Check terminal for “Classification counts” and open `resulting_csv/lisa_test_report.csv` for new columns:
   - `derived_submitted_at`, `classification_source`, `late_flag`, `days_late`, `rules_version`

Edit Here
- `helpers/derivations.py` — stop at first matching rule:
  1) submitted_at → use it
  2) graded_no_submit → use graded_at
  3) lti_external → attempt/attachments present
  4) no_submission_required → don’t mark late
  5) late_override → tag when policy fields present
  6) unknown → do not guess

Notes
- IDs as strings; timestamps normalized to UTC inside the helper.
- When unsure, use `unknown` and keep raw fields intact (transparency > false precision).

