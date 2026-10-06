"""Reader side of the dummy conditions DB the fake upload writes.

The on-disk formats (the provenance manifest a fake cmsRun output carries, and
the $NGT_DEV_HOME/conddb/payloads.jsonl ledger) are defined once, in
scenario-player/bin/_fakeprov.py, because the *writers* are the fake binaries
in bin/ and those have to work from a copy of that directory with nothing but
the standard library importable -- see that module's docstring. bin/ is this
file's sibling, so this module imports it rather than restating any of it:
there is one implementation of the format, not two to drift apart.

What this module adds on top is the vocabulary the player side needs and the
fake binaries do not: lumisection-range parsing (`51-53`, `[51,52,53]`,
`"51-53,57"`) and the inverse, compact formatting for a failure report.
"""
import os
import sys

_BIN_DIR = os.path.join(os.path.dirname(os.path.realpath(__file__)), "bin")
if _BIN_DIR not in sys.path:
    sys.path.insert(0, _BIN_DIR)

import _fakeprov  # noqa: E402  -- scenario-player/bin/_fakeprov.py, the format's single definition

# Re-exported so callers (scenario_player.py, expectations.py, the tests) have
# one obvious import for all of this and never reach into bin/ themselves.
CONDDB_DIR_NAME = _fakeprov.CONDDB_DIR_NAME
LEDGER_NAME = _fakeprov.LEDGER_NAME
PROVENANCE_MAGIC = _fakeprov.PROVENANCE_MAGIC

append_payload = _fakeprov.append_payload
clear_ledger = _fakeprov.clear_ledger
conddb_dir = _fakeprov.conddb_dir
ledger_path = _fakeprov.ledger_path
read_ledger = _fakeprov.read_ledger
read_provenance = _fakeprov.read_provenance
write_provenance = _fakeprov.write_provenance


class LumisectionSpecError(ValueError):
    """Raised for an unparseable `lumisections:` value in an `expect:` entry."""


def _require_positive_ls(value):
    """Lumisection numbering starts at 1, so a zero or negative one is a typo
    (most often a stray leading '-', which would otherwise parse as a
    perfectly valid negative integer and silently never match)."""
    if value < 1:
        raise LumisectionSpecError(f"lumisections: {value} is not a valid lumisection number (they start at 1)")
    return value


def parse_ls_spec(spec):
    """A `lumisections:` value -> a set of ints.

    Accepts a bare int (`51`), a list of ints or range strings
    (`[51, 52]`, `[51, "60-62"]`), or a string of comma-separated ints and
    inclusive ranges (`"51-53"`, `"51-53,57"`). Inclusive because that is how
    a lumisection range is written and read everywhere else in CMS.
    """
    if spec is None:
        raise LumisectionSpecError("lumisections: missing (expected e.g. 51, '51-53' or [51, 52, 53])")
    if isinstance(spec, bool):
        raise LumisectionSpecError(f"lumisections: expected numbers or ranges, got {spec!r}")
    if isinstance(spec, int):
        return {_require_positive_ls(spec)}
    if isinstance(spec, (list, tuple, set)):
        merged = set()
        for item in spec:
            merged |= parse_ls_spec(item)
        return merged
    if not isinstance(spec, str):
        raise LumisectionSpecError(f"lumisections: expected a number, range string or list, got {spec!r}")

    merged = set()
    for part in spec.split(","):
        part = part.strip()
        if not part:
            continue
        if "-" in part.lstrip("-"):
            low_raw, _, high_raw = part.partition("-")
            try:
                low, high = int(low_raw), int(high_raw)
            except ValueError:
                raise LumisectionSpecError(f"lumisections: {part!r} is not a valid range (expected e.g. '51-53')")
            if high < low:
                raise LumisectionSpecError(f"lumisections: range {part!r} ends before it starts")
            merged.update(range(_require_positive_ls(low), _require_positive_ls(high) + 1))
        else:
            try:
                value = int(part)
            except ValueError:
                raise LumisectionSpecError(f"lumisections: {part!r} is not a lumisection number")
            merged.add(_require_positive_ls(value))
    if not merged:
        raise LumisectionSpecError(f"lumisections: {spec!r} describes no lumisections")
    return merged


def format_ls_set(lumisections):
    """The inverse of parse_ls_spec, collapsing runs of consecutive numbers:
    {51,52,53,57} -> "51-53,57". Used in failure reports, where a bare
    30-element list would bury the one number that differs."""
    remaining = sorted(set(lumisections))
    if not remaining:
        return "(none)"
    groups = []
    start = previous = remaining[0]
    for value in remaining[1:]:
        if value == previous + 1:
            previous = value
            continue
        groups.append((start, previous))
        start = previous = value
    groups.append((start, previous))
    return ",".join(str(low) if low == high else f"{low}-{high}" for low, high in groups)
