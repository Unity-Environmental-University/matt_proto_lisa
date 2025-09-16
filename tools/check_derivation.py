import sys
import csv
from collections import Counter
from helpers.derivations import derive_submission


def main(path: str) -> int:
    counts = Counter()
    preview = []
    with open(path, newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for i, row in enumerate(reader, start=1):
            d = derive_submission(row, due_at=row.get('due_at'))
            counts[d['classification_source']] += 1
            if i <= 5:
                preview.append({
                    'id': row.get('id'),
                    'assignment_id': row.get('assignment_id'),
                    'derived_submitted_at': d['derived_submitted_at'].isoformat() if d['derived_submitted_at'] else None,
                    'classification_source': d['classification_source'],
                    'late_flag': d['late_flag'],
                    'days_late': d['days_late'],
                })

    print('[BRO NOTE] Classification counts:')
    for k, v in counts.items():
        print(f'  {k:22s} {v}')
    print('\n[BRO NOTE] Preview (first 5 rows):')
    for item in preview:
        print(' ', item)
    return 0


if __name__ == '__main__':
    if len(sys.argv) < 2:
        print('usage: python tools/check_derivation.py fixtures/submission_derivation_sample.csv')
        sys.exit(2)
    sys.exit(main(sys.argv[1]))

