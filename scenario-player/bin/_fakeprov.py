"""
(FAKE) provenance manifests + the dummy conditions-DB ledger: the single
source of truth for both on-disk formats the fake toolchain produces.

Why this lives in bin/ rather than beside scenario-player/faults.py: the fake
binaries are invoked by a bare $PATH lookup, from a *copy* of this directory
(scenario-player/sim_env.sh's sim_setup does `cp scenario-player/bin/*
"$NGT_DEV_HOME/bin/"`), with nothing but the standard library guaranteed
importable and no guarantee that scenario-player/ itself is on sys.path. A
sibling module found via __file__ is the one import that always works from
there -- which is why scenario-player/bin/cmsRun had to *duplicate*
faults.py's consume_fault rather than import it. Keeping the formats here
instead means scenario-player/conddb.py can import this module (bin/ is its
own sibling directory) and there is exactly one implementation, with no
duplicated copy to drift.

Two formats are defined:

1. A *provenance manifest*, which is what a fake output file now contains
   instead of an opaque placeholder byte string: a magic first line followed
   by one JSON object naming the calibration/run/lumisections that actually
   went into producing it. scenario-player/bin/cmsRun writes one per output
   and reads its inputs' manifests back, so identifiers propagate along the
   real data path step2 -> step3 -> step4 rather than being invented at the
   end. Raw "EOS" files seeded by scenario-player/seed.py have no manifest --
   their identity is the run<RUN>_ls<LS> filename convention that
   scenario-player/bin/edmFileUtil already parses, which is the chain's root
   and the fallback in lumisections_of_input().

2. The *conditions ledger*, $NGT_DEV_HOME/conddb/payloads.jsonl: one JSON
   object per successful upload, appended under flock by
   scenario-player/bin/uploadConditions.py. JSON Lines rather than a real
   sqlite file because an append-only single-line write under a lock is safe
   across the separate OS processes doing the uploading (Airflow's
   scheduler/triggerer/worker, or several calibrations at once), preserves
   insertion order -- which is what an ordered expectation check needs -- and
   stays greppable with `tail -f` during a live demo.
"""
import json
import os
import re
from datetime import datetime, timezone

PROVENANCE_MAGIC = "(FAKE-NGT-PROVENANCE-v1)"
CONDDB_DIR_NAME = "conddb"
LEDGER_NAME = "payloads.jsonl"

# The raw-RAW-file naming convention scenario-player/seed.py's _touch_ls_file
# writes and scenario-player/bin/edmFileUtil parses: run<RUN>_ls<LS>....root.
RAW_LS_RE = re.compile(r"run(\d+)_ls(\d+)")

# seq first so a hand-read of the ledger scans in upload order; the rest in
# the order a human debugging a failed expectation wants them.
LEDGER_FIELD_ORDER = (
    "seq",
    "uploaded_at",
    "calibration",
    "run",
    "lumisections",
    "n_input_files",
    "db_file",
    "job_dir",
)


def utc_now():
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _as_int(value):
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


# --------------------------------------------------------------------------
# Provenance manifests (what a fake cmsRun output file contains)
# --------------------------------------------------------------------------


def write_provenance(path, *, step, calibration, run, lumisections, inputs):
    """Write one output file as a provenance manifest. Returns the manifest."""
    manifest = {
        "step": step,
        "calibration": calibration,
        "run": _as_int(run),
        "lumisections": sorted(lumisections),
        "inputs": sorted(os.path.basename(str(p)) for p in inputs),
        "produced_at": utc_now(),
    }
    with open(path, "w", encoding="utf-8") as f:
        f.write(PROVENANCE_MAGIC + "\n")
        json.dump(manifest, f, indent=2)
        f.write("\n")
    return manifest


def read_provenance(path):
    """The manifest dict in `path`, or None if it isn't one of ours (a raw
    seeded file, a real file, or anything unreadable)."""
    try:
        with open(path, "r", encoding="utf-8") as f:
            if f.readline().strip() != PROVENANCE_MAGIC:
                return None
            return json.load(f)
    except (OSError, UnicodeDecodeError, json.JSONDecodeError):
        return None


def lumisections_of_input(path):
    """The lumisections one cmsRun input contributes: its manifest's set if it
    has one, else the single LS encoded in a raw file's name. An unreadable or
    unrecognised input contributes nothing, mirroring how
    scenario-player/bin/edmFileUtil reports "ERR" for a file it cannot read."""
    manifest = read_provenance(path)
    if manifest is not None:
        return {ls for ls in (_as_int(x) for x in manifest.get("lumisections") or []) if ls is not None}
    match = RAW_LS_RE.search(os.path.basename(str(path)))
    return {int(match.group(2))} if match else set()


def merge_input_lumisections(paths):
    """Union of every input's lumisections -- what a step's output covers."""
    merged = set()
    for path in paths:
        merged |= lumisections_of_input(path)
    return merged


# --------------------------------------------------------------------------
# The dummy conditions DB ($NGT_DEV_HOME/conddb/payloads.jsonl)
# --------------------------------------------------------------------------


def conddb_dir():
    explicit = os.environ.get("NGT_CONDDB_DIR")
    if explicit:
        return explicit
    return os.path.join(os.environ.get("NGT_DEV_HOME", "."), CONDDB_DIR_NAME)


def ledger_path():
    return os.path.join(conddb_dir(), LEDGER_NAME)


def order_row(row):
    """LEDGER_FIELD_ORDER first, anything else appended -- readability only."""
    ordered = {key: row[key] for key in LEDGER_FIELD_ORDER if key in row}
    ordered.update({key: value for key, value in row.items() if key not in ordered})
    return ordered


def read_ledger(path=None):
    """Every upload row, oldest first. A torn trailing line (a writer killed
    mid-append) is skipped rather than raising -- the ledger is read while
    uploads may still be landing."""
    path = path or ledger_path()
    if not os.path.exists(path):
        return []
    rows = []
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                rows.append(json.loads(line))
            except json.JSONDecodeError:
                continue
    rows.sort(key=lambda row: row.get("seq") or 0)
    return rows


def append_payload(record, path=None):
    """Append one upload row, assigning `seq` inside the lock so it is a true
    global upload order even with several calibrations uploading at once.
    Returns the row as written.

    POSIX-only (fcntl.flock), imported lazily so this module still imports on
    Windows -- same reasoning as scenario-player/faults.py's locked_json_file.
    """
    import fcntl

    path = path or ledger_path()
    directory = os.path.dirname(path) or "."
    os.makedirs(directory, exist_ok=True)

    lock_path = path + ".lock"
    with open(lock_path, "a+") as lock_file:
        fcntl.flock(lock_file, fcntl.LOCK_EX)
        try:
            row = dict(record)
            row["seq"] = len(read_ledger(path)) + 1
            row.setdefault("uploaded_at", utc_now())
            row = order_row(row)
            with open(path, "a", encoding="utf-8") as f:
                f.write(json.dumps(row) + "\n")
            return row
        finally:
            fcntl.flock(lock_file, fcntl.LOCK_UN)


def clear_ledger(path=None):
    """Drop the ledger (and its lock file). Covered for free by sim_env.sh's
    wholesale `rm -rf $NGT_DEV_HOME` reset; this is for clearing uploads
    mid-session without resetting run/EOS state too, mirroring
    scenario-player/seed.py's clear_faults."""
    path = path or ledger_path()
    for candidate in (path, path + ".lock"):
        try:
            os.unlink(candidate)
        except OSError:
            pass
