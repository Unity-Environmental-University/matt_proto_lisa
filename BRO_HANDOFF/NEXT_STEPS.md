[BRO NOTE] Next Steps (use if helpful)

1) Sanity-check the helper
   - `python tools/check_derivation.py fixtures/submission_derivation_sample.csv`
   - Adjust `helpers/derivations.py` until counts/preview match your expectations.

2) Run your normal script
   - Confirm CSV now includes: derived_submitted_at, classification_source, late_flag, days_late, rules_version.

3) Iterate rules (conservative first)
   - Prefer `unknown` over guessing. Add signals as you learn (attempts/attachments, policy flags).

4) (Later) If you want DB integration
   - We’ll plug the same rules into the backend’s `CanvasRecordFactory` submission path.

