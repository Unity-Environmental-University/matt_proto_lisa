from datetime import datetime, timezone
from math import ceil
from typing import Any, Dict, Optional


# [BRO NOTE] This helper is intentionally minimal and conservative.
# It adds transparent, auditable fields without changing your flow.
RULES_VERSION = 1


def _to_dt_utc(value: Optional[str]) -> Optional[datetime]:
    """[BRO NOTE] Parse ISO-like timestamp to aware UTC datetime.
    Returns None on failure. Naive times are treated as UTC.
    """
    if not value:
        return None
    try:
        # pandas may already parse to datetime; support passthrough
        if isinstance(value, datetime):
            return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
        s = str(value).replace('Z', '+00:00')
        dt = datetime.fromisoformat(s)
        return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
    except Exception:
        return None


def derive_submission(row: Dict[str, Any], *, due_at: Optional[str] = None,
                      assignment_flags: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """[BRO NOTE] Derive submitted_at classification + lateness with minimal guessing.

    Inputs expected in row (use what you have):
    - submitted_at, graded_at, workflow_state, submission_type, attempt/attachments (optional)
    - Optional: due_at (string) or pass via kwarg; assignment_flags like {'no_submission': True}

    Returns keys:
    - derived_submitted_at (datetime|None, UTC)
    - classification_source (one of: submitted_at, graded_no_submit, lti_external,
      no_submission_required, late_override, unknown)
    - late_flag (bool)
    - days_late (int|None)
    - rules_version (int)
    """
    subm_at = _to_dt_utc(row.get('submitted_at'))
    graded_at = _to_dt_utc(row.get('graded_at'))
    # due_at can live in row or be provided
    due_val = due_at or row.get('due_at')
    due_dt = _to_dt_utc(due_val)
    submission_type = (row.get('submission_type') or '').lower()
    workflow_state = (row.get('workflow_state') or '').lower()
    flags = assignment_flags or {}
    no_submission = bool(flags.get('no_submission') or row.get('no_submission'))

    # 1) direct submitted_at
    if subm_at:
        source = 'submitted_at'
        derived = subm_at
    # 2) graded without submit
    elif graded_at and ('graded' in workflow_state or workflow_state in {'graded', 'returned', 'complete'}):
        source = 'graded_no_submit'
        derived = graded_at
    # 3) external tool/url with any signal of attempt
    elif submission_type in {'external_tool', 'online_url'} and (row.get('attempt') or row.get('attachments')):
        source = 'lti_external'
        # best available ts
        derived = subm_at or graded_at
    # 4) assignment does not require submission
    elif no_submission:
        source = 'no_submission_required'
        derived = None
    # 5) late override present (placeholder: detect by explicit fields if available)
    elif row.get('late_policy_status') or row.get('seconds_late'):
        source = 'late_override'
        derived = subm_at or graded_at
    else:
        source = 'unknown'
        derived = None

    # lateness calc
    late_flag = False
    days_late = None
    if derived and due_dt:
        late_flag = derived > due_dt
        if late_flag:
            delta = derived - due_dt
            days_late = max(1, ceil(delta.total_seconds() / 86400))
    elif source == 'no_submission_required':
        late_flag = False

    return {
        'derived_submitted_at': derived,
        'classification_source': source,
        'late_flag': late_flag,
        'days_late': days_late,
        'rules_version': RULES_VERSION,
    }
