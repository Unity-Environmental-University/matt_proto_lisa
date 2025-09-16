[BRO NOTE] Small, Safe Ideas (optional)

- Add per-classification counts to your CSV footer or a sidecar JSON for quick QA.
- Emit a simple `provenance_note` when you tag `unknown` (e.g., which fields were missing).
- Add a flag to `matt_mini_test.py` to run on a small random sample before full runs.
- If you want a DB on-ramp later: chunked CSV → factory.normalize_submission → bulk insert.

