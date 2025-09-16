Hey Matt — BRO here. Quick setup without taking the wheel.

- Added a tiny helper (`helpers/derivations.py`) so you can classify submissions without changing your flow. It normalizes times to UTC and adds clear audit columns to your CSV.
- Run `python tools/check_derivation.py fixtures/submission_derivation_sample.csv` for a fast sanity check, then run your normal script. You’ll see a classification count print, and the CSV will include the new columns.
- If anything feels off, only change `helpers/derivations.py`. Prefer `classification_source='unknown'` over guessing.
- When you’re ready, we’ll drop the same rules into the backend’s streaming factory so rows land in DB safely, no RAM drama.

You got this. [BRO NOTE] marks my comments; delete or ignore whenever you want.

