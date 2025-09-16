[BRO NOTE] Integration Notes (later)

When ready to flow into the backend DB without RAM spikes:

Path
- CSV (chunks) → normalize via a shared `CanvasRecordFactory.normalize_submission` → ORM → bulk insert (commit per N rows).
- DAP streaming uses the same factory path; no duplicated mapping.

Fields
- Store both raw and derived fields for submissions:
  - Raw: submitted_at, graded_at, workflow_state, submission_type
  - Derived: derived_submitted_at, submitted_at_source, late_flag, days_late, derivation_rules_version

Hooks
- Factory: `LisaEnginePrototype/services/canvas_record_factory.py` (normalize_submission)
- JSON storage/versioning: `models/json_data_haver_mixin.py` (keep versioning lean)
- Metrics: `services/compliance_service.py`

Flags (for a soft landing — add when integrating)
- `DERIVATION_ENABLED=0|1` to toggle using derived fields in the streamer.
- `INGEST_SOURCE=matt_csv` to tag provenance of ingested rows.

