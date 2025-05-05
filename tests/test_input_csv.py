import pytest
import csv
import io

# --- test_csv_input_format ---
def test_csv_input_format():
    sample = "name,website\nRoaster A,https://a.com\nRoaster B,https://b.com\n"
    reader = csv.DictReader(io.StringIO(sample))
    rows = list(reader)
    assert all('name' in row and 'website' in row for row in rows)
    assert len(rows) == 2

# --- test_csv_input_edge_cases ---
def test_csv_input_edge_cases():
    # Special chars, duplicate, empty row
    sample = "name,website\nRoaster A,https://a.com\nRoaster B,https://b.com\nRoaster A,https://a.com\n,\n"
    reader = csv.DictReader(io.StringIO(sample))
    rows = [row for row in reader if row['name'] and row['website']]
    assert len(rows) == 3  # Excludes empty row
    assert rows[0]['name'] == 'Roaster A'
    assert rows[2]['name'] == 'Roaster A'  # Duplicate present
