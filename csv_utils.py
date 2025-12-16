# csv_utils.py
import csv
import os
from typing import List, Dict

def load_csv_rows(path: str) -> List[Dict]:
    if not os.path.isfile(path):
        return []
    with open(path, "r", newline="") as f:
        return list(csv.DictReader(f))

def write_csv_rows(path: str, rows: List[Dict], fieldnames: List[str]) -> None:
    with open(path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for r in rows:
            w.writerow(r)

def append_csv_row(path: str, row: Dict, fieldnames: List[str]) -> None:
    file_exists = os.path.isfile(path)
    with open(path, "a", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        if not file_exists:
            w.writeheader()
        w.writerow(row)
