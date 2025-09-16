[BRO NOTE] Handoff Packet for Matt

Purpose
- Set you up for success fast, without taking your keyboard. This is a guide and a few small tools; you own the logic and pace.

What’s Inside
- SCAFFOLDING.md — quick-start, commands, and where the helper is wired.
- LINKS.md — pointers to backend files when you’re ready to integrate.
- GOTCHAS.md — common traps (IDs, timezones, memory) and how to avoid them.
- IDEAS.md — small, safe ideas if you want to extend.
- NEXT_STEPS.md — a short checklist you can follow or ignore.
- INTEGRATION_NOTES.md — where this plugs into the streaming backend (later).
- HI_MATT.md — quick note from BRO with context.

TL;DR
- Run: `python tools/check_derivation.py fixtures/submission_derivation_sample.csv`
- Then run your normal script; check the printed classification counts and the new columns in the CSV.
- If something feels off, only edit `helpers/derivations.py`. Prefer `classification_source='unknown'` over guessing.

