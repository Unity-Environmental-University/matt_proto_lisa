[BRO NOTE] Gotchas / Traps to Avoid

Data
- IDs as strings: Canvas IDs can be large and lose fidelity if coerced to ints.
- Timezones: normalize to UTC before lateness comparisons (helper does this).
- External tools: attempts/attachments are better signals than `submitted_at`.
- No-submission assignments: don’t count them as late.

Performance
- Don’t load giant CSVs fully; if/when ingesting to DB, read in chunks.
- Keep derivation small and pure; vectorized `DataFrame.apply` is fine for now.

Versioning / Provenance
- Prefer `classification_source='unknown'` over guessing.
- If rules change, bump `RULES_VERSION` in `helpers/derivations.py`.

Scope
- Avoid touching backend demo endpoints; they can confuse expectations.
- Avoid migrations/old sprint scripts for now; they’re context, not tasks.

