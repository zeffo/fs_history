# fs_history

## Setup

1. Install poetry: `python -m pip install -U pip`
2. Install dependencies: `python -m poetry install`
3. Run: `python -m poetry run script.py`

## Sample

```py
from fs_history import Database
from pathlib import Path

db = Database("mysql://root@localhost") # accepts any database connection URL

db.setup()  # create tables

for file in Path(".").glob("*"):
    db.upsert_version(file, {"some": "data"})   # automatically calculates latest version and inserts the data

# select all:
for rec in db.select_all():
    print(rec)

# select paths:
for path in db.select_paths():  # can specify `parent` and `name` to filter
    print(path)

# select versions:
for version in db.select_versions():    # can specify `path_id` and `version_no` to filter
    print(version)

# drop tables
db.drop()

# Inserting a Path/Version
path = db.insert_path(parent="/usr/aman", name="test.txt")
version = db.insert_version(path_id=path.id, version_no=1, attrs={"some": "data"})

```