"""Declarative expected results for a scenario, and the check that turns one
into a pass/fail verdict.

A scenario YAML gains an `expect:` block alongside `runs:` and `faults:`,
describing which payloads should have reached the dummy conditions DB
(scenario-player/conddb.py) by the time playback finishes:

    expect:
      settle_timeout: 300          # cap on waiting for uploads to arrive
      quiet_for: 20                # settled once no new upload lands for this long

      allow_missing_lumisections: false
      allow_extra_lumisections: false
      allow_duplicates: false      # two uploads carrying an identical LS set
      ordered: false               # listed entries must land in the listed order
      allow_extra:                 # unlisted *uploads*; scalar true/false sets all four
        payloads: false            #   a (calibration, run) appearing in no entry at all
        before: false              #   extra uploads for a listed key, before its first match
        between: false             #   ... interleaved between its matches
        after: false               #   ... after its last match

      payloads:
        - {calibration: EcalPedestals, run: 398600, lumisections: 51-53}

`payloads:` is the expected *sequence* of uploads, not just the end state:
every ledger row must be either matched by an entry or permitted by an
`allow_extra` category. To assert only the final payload, list just that one
and allow what precedes it -- `allow_extra: {before: true}` -- which reads as
"the run must end with exactly this payload, intermediate re-harvests are
fine, and nothing may be uploaded afterwards".

Why `before` and `after` are separate knobs: they mean different things. An
extra upload *before* the expected one is usually benign, because Step 4
deliberately re-harvests everything available on each cycle rather than just
the new delta (ngt_calibration_loop/step4.py's check_files_for_processing),
so a healthy run climbs through growing payloads. An extra upload *after* it
is usually a real defect -- a payload landing once the run has already ended,
or a DAG re-firing on a coalesced asset event. A scenario can tolerate the
first while still failing on the second.

Every flag may also be set *inside* a `payloads` entry, overriding the
block-level default for that one payload (`ordered` and `allow_extra.payloads`
excepted -- they are statements about the list as a whole, not about one
entry).

This module has no Airflow, OMS or CMSSW dependency, and no knowledge of how
the uploads were produced -- it only reads the ledger.
"""
import time
from dataclasses import dataclass, field, replace
from typing import Optional, Tuple

import conddb

DEFAULT_SETTLE_TIMEOUT = 300.0
DEFAULT_QUIET_FOR = 20.0
DEFAULT_POLL_INTERVAL = 2.0

EXTRA_CATEGORIES = ("payloads", "before", "between", "after")
_ENTRY_TOLERANCE_FIELDS = (
    "allow_missing_lumisections",
    "allow_extra_lumisections",
    "allow_duplicates",
)


class ExpectationError(ValueError):
    """Raised for an invalid `expect:` block, naming the offending field."""


def _require(condition, message):
    if not condition:
        raise ExpectationError(message)


def _as_int(value):
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _require_bool(value, name):
    _require(isinstance(value, bool), f"expect: {name!r} must be true or false, got {value!r}")
    return value


def _require_positive_number(value, name):
    _require(
        isinstance(value, (int, float)) and not isinstance(value, bool) and value > 0,
        f"expect: {name!r} must be a positive number, got {value!r}",
    )
    return float(value)


@dataclass(frozen=True)
class Tolerances:
    allow_missing_lumisections: bool = False
    allow_extra_lumisections: bool = False
    allow_duplicates: bool = False
    extra_before: bool = False
    extra_between: bool = False
    extra_after: bool = False


@dataclass(frozen=True)
class PayloadExpectation:
    calibration: str
    run: int
    lumisections: frozenset
    tolerances: Tolerances
    index: int = 0  # position in the `payloads:` list, for `ordered`

    @property
    def key(self):
        return (self.calibration, self.run)

    def describe(self):
        return f"{self.calibration}/run{self.run} LS {conddb.format_ls_set(self.lumisections)}"

    def compare(self, row) -> Tuple[set, set]:
        """(missing, extra) lumisections for one candidate ledger row."""
        actual = {ls for ls in (_as_int(x) for x in row.get("lumisections") or []) if ls is not None}
        return set(self.lumisections) - actual, actual - set(self.lumisections)

    def accepts(self, row) -> bool:
        missing, extra = self.compare(row)
        if missing and not self.tolerances.allow_missing_lumisections:
            return False
        if extra and not self.tolerances.allow_extra_lumisections:
            return False
        return True


@dataclass(frozen=True)
class Expectations:
    payloads: tuple = ()
    defaults: Tolerances = field(default_factory=Tolerances)
    allow_extra_payloads: bool = False
    ordered: bool = False
    settle_timeout: float = DEFAULT_SETTLE_TIMEOUT
    quiet_for: float = DEFAULT_QUIET_FOR
    poll_interval: float = DEFAULT_POLL_INTERVAL

    def describe(self):
        if not self.payloads:
            return "expect: no payloads should reach the conditions DB"
        return "expect: " + "; ".join(entry.describe() for entry in self.payloads)


def _parse_allow_extra(raw, name="allow_extra"):
    """-> (payloads, before, between, after). A scalar sets all four."""
    if raw is None:
        return (False, False, False, False)
    if isinstance(raw, bool):
        return (raw, raw, raw, raw)
    _require(
        isinstance(raw, dict),
        f"expect: {name!r} must be true/false or a map of {list(EXTRA_CATEGORIES)}, got {raw!r}",
    )
    raw = dict(raw)
    values = []
    for category in EXTRA_CATEGORIES:
        values.append(_require_bool(raw.pop(category, False), f"{name}.{category}"))
    _require(not raw, f"expect: unknown {name!r} categor(ies) {sorted(raw)}; valid: {list(EXTRA_CATEGORIES)}")
    return tuple(values)


def _parse_tolerances(raw, base: Tolerances, *, where, allow_payloads_category):
    """Pull the tolerance flags out of `raw` (mutating it), defaulting to
    `base` for anything absent. Returns (Tolerances, allow_extra_payloads),
    the latter only ever true when allow_payloads_category is set."""
    values = {}
    for name in _ENTRY_TOLERANCE_FIELDS:
        if name in raw:
            values[name] = _require_bool(raw.pop(name), f"{where}.{name}")

    extra_payloads = False
    if "allow_extra" in raw:
        payloads, before, between, after = _parse_allow_extra(raw.pop("allow_extra"), f"{where}.allow_extra")
        _require(
            allow_payloads_category or not payloads,
            f"expect: {where}.allow_extra.payloads is not meaningful on a single entry -- it is about "
            "whole (calibration, run) keys that no entry lists, so set it on the expect: block itself",
        )
        extra_payloads = payloads and allow_payloads_category
        values.update({"extra_before": before, "extra_between": between, "extra_after": after})

    return replace(base, **values), extra_payloads


def parse_expectations(raw: dict) -> Expectations:
    """Validate an `expect:` block (a plain dict from YAML) into Expectations.

    Raises ExpectationError, naming the offending field, on anything invalid --
    so a typo in a scenario file fails fast with a readable message instead of
    silently weakening the check it was meant to express.
    """
    _require(isinstance(raw, dict), f"expect: must be a mapping, got {raw!r}")
    raw = dict(raw)

    settle_timeout = _require_positive_number(raw.pop("settle_timeout", DEFAULT_SETTLE_TIMEOUT), "settle_timeout")
    quiet_for = _require_positive_number(raw.pop("quiet_for", DEFAULT_QUIET_FOR), "quiet_for")
    poll_interval = _require_positive_number(raw.pop("poll_interval", DEFAULT_POLL_INTERVAL), "poll_interval")
    ordered = _require_bool(raw.pop("ordered", False), "ordered")

    defaults, allow_extra_payloads = _parse_tolerances(
        raw, Tolerances(), where="expect", allow_payloads_category=True
    )

    payloads_raw = raw.pop("payloads", None)
    _require(
        payloads_raw is not None,
        "expect: 'payloads' is required -- use 'payloads: []' to assert that nothing should be uploaded",
    )
    _require(isinstance(payloads_raw, list), f"expect: 'payloads' must be a list, got {payloads_raw!r}")

    _require(not raw, f"expect: unknown field(s) {sorted(raw)}")

    payloads = []
    for index, entry_raw in enumerate(payloads_raw):
        _require(isinstance(entry_raw, dict), f"expect: payloads[{index}] must be a mapping, got {entry_raw!r}")
        entry_raw = dict(entry_raw)
        where = f"payloads[{index}]"

        calibration = entry_raw.pop("calibration", None)
        _require(
            isinstance(calibration, str) and calibration,
            f"expect: {where} requires a 'calibration' (e.g. 'EcalPedestals')",
        )
        run = entry_raw.pop("run", None)
        _require(_as_int(run) is not None, f"expect: {where} requires an integer 'run', got {run!r}")

        try:
            lumisections = conddb.parse_ls_spec(entry_raw.pop("lumisections", None))
        except conddb.LumisectionSpecError as exc:
            raise ExpectationError(f"expect: {where} {exc}") from exc

        tolerances, _ = _parse_tolerances(entry_raw, defaults, where=where, allow_payloads_category=False)
        _require(not entry_raw, f"expect: unknown field(s) {sorted(entry_raw)} in {where}")

        payloads.append(
            PayloadExpectation(
                calibration=calibration,
                run=int(run),
                lumisections=frozenset(lumisections),
                tolerances=tolerances,
                index=index,
            )
        )

    return Expectations(
        payloads=tuple(payloads),
        defaults=defaults,
        allow_extra_payloads=allow_extra_payloads,
        ordered=ordered,
        settle_timeout=settle_timeout,
        quiet_for=quiet_for,
        poll_interval=poll_interval,
    )


@dataclass
class Verdict:
    ok: bool
    problems: list = field(default_factory=list)
    rows: list = field(default_factory=list)
    settled: bool = True


def _row_key(row):
    return (row.get("calibration"), _as_int(row.get("run")))


def _row_ls(row):
    return {ls for ls in (_as_int(x) for x in row.get("lumisections") or []) if ls is not None}


def _describe_row(row):
    return (
        f"seq {row.get('seq')}  {row.get('calibration')}/run{row.get('run')}  "
        f"LS {conddb.format_ls_set(_row_ls(row))}"
    )


def _group_rows(rows):
    grouped = {}
    for row in rows:
        grouped.setdefault(_row_key(row), []).append(row)
    for key_rows in grouped.values():
        key_rows.sort(key=lambda row: row.get("seq") or 0)
    return grouped


def _align(entries, key_rows, problems):
    """Match each entry, in listed order, to the next row that satisfies it.
    Returns {row index -> entry} for the matches made. A row skipped while
    searching stays unmatched and is classified as an extra afterwards; the
    cursor never moves backwards, which is what enforces ordering within one
    (calibration, run)."""
    matches = {}
    cursor = 0
    for entry in entries:
        found = None
        for i in range(cursor, len(key_rows)):
            if entry.accepts(key_rows[i]):
                found = i
                break
        if found is None:
            problems.append(_missing_problem(entry, key_rows[cursor:]))
            continue
        matches[found] = entry
        cursor = found + 1
    return matches


def _missing_problem(entry, candidate_rows):
    """Why no upload satisfied `entry`, quoting the closest candidate so the
    report shows the actual difference rather than just "not found"."""
    if not candidate_rows:
        return f"{entry.describe()}: no matching upload (none left for this run)"
    best = min(candidate_rows, key=lambda row: len(entry.compare(row)[0]) + len(entry.compare(row)[1]))
    missing, extra = entry.compare(best)
    bits = []
    if missing:
        bits.append(f"missing LS {conddb.format_ls_set(missing)}")
    if extra:
        bits.append(f"unexpected LS {conddb.format_ls_set(extra)}")
    detail = "; ".join(bits) or "no difference in lumisections"
    return f"{entry.describe()}: no matching upload -- closest was seq {best.get('seq')} ({detail})"


def _classify_extras(matches, key_rows, tolerances, problems):
    if not matches:
        return
    first, last = min(matches), max(matches)
    for i, row in enumerate(key_rows):
        if i in matches:
            continue
        if i < first:
            category, allowed = "before", tolerances.extra_before
        elif i > last:
            category, allowed = "after", tolerances.extra_after
        else:
            category, allowed = "between", tolerances.extra_between
        if not allowed:
            problems.append(
                f"unexpected upload {category} the expected payload(s): {_describe_row(row)} "
                f"(allow with allow_extra.{category})"
            )


def _check_duplicates(key_rows, tolerances, problems):
    """A duplicate is two uploads for one (calibration, run) carrying an
    *identical* lumisection set -- not merely a second upload. Growing
    re-harvests ({51} then {51,52}) are the pipeline working as designed; the
    same set twice means an upload with no new statistics behind it, which
    Step 4 should never produce since it only starts a cycle when there is
    something new to harvest."""
    if tolerances.allow_duplicates:
        return
    seen = {}
    for row in key_rows:
        signature = frozenset(_row_ls(row))
        if signature in seen:
            problems.append(
                f"duplicate upload: seq {row.get('seq')} repeats the lumisection set already uploaded at "
                f"seq {seen[signature]} ({row.get('calibration')}/run{row.get('run')} "
                f"LS {conddb.format_ls_set(signature)}) -- allow with allow_duplicates: true"
            )
        else:
            seen[signature] = row.get("seq")


def _check_order(all_matches, problems):
    """`ordered` only governs entries belonging to *different* keys -- order
    within one key is already enforced by _align's forward-only cursor."""
    in_listed_order = [seq for _, seq in sorted(all_matches, key=lambda pair: pair[0])]
    for earlier, later in zip(in_listed_order, in_listed_order[1:]):
        if later < earlier:
            problems.append(
                f"uploads landed out of the expected order: an entry matched seq {later} after an "
                f"earlier entry matched seq {earlier} (relax with ordered: false)"
            )
            return


def evaluate(expectations: Expectations, rows) -> Verdict:
    """Check a ledger (as read by conddb.read_ledger) against Expectations."""
    problems = []
    rows_by_key = _group_rows(rows)

    entries_by_key = {}
    for entry in expectations.payloads:
        entries_by_key.setdefault(entry.key, []).append(entry)

    all_matches = []
    for key, entries in entries_by_key.items():
        key_rows = rows_by_key.get(key, [])
        matches = _align(entries, key_rows, problems)
        # The extra-upload and duplicate checks are properties of a whole
        # (calibration, run), not of one entry, so they read the *first*
        # listed entry's tolerances for that key -- identical to the block
        # defaults unless that entry overrode them. In the documented idiom
        # (one entry per key) that is simply "this payload's own flags".
        key_tolerances = entries[0].tolerances
        _classify_extras(matches, key_rows, key_tolerances, problems)
        _check_duplicates(key_rows, key_tolerances, problems)
        for row_index, entry in matches.items():
            all_matches.append((entry.index, key_rows[row_index].get("seq") or 0))

    for key, key_rows in rows_by_key.items():
        if key in entries_by_key:
            continue
        if expectations.allow_extra_payloads:
            continue
        calibration, run = key
        problems.append(
            f"unexpected payload {calibration}/run{run}: {len(key_rows)} upload(s) reached the "
            f"conditions DB but no expect: entry describes it (allow with allow_extra.payloads) -- "
            f"first was {_describe_row(key_rows[0])}"
        )

    if expectations.ordered:
        _check_order(all_matches, problems)

    return Verdict(ok=not problems, problems=problems, rows=list(rows))


def wait_for_settle(expectations: Expectations, log=print, sleep=time.sleep, clock=time.monotonic):
    """Poll the ledger until it stops growing, or settle_timeout elapses.

    Settling on *quiet* rather than on "the expectations are satisfied" is
    deliberate: returning as soon as the expected payloads appear would make
    `allow_extra.after: false` unenforceable, since a late, unwanted upload
    would land after the check had already passed. The cost is that a passing
    scenario always spends its last `quiet_for` seconds waiting.

    Returns (settled, rows): settled is False if the deadline hit while
    uploads were still arriving, which evaluate()'s caller reports as a
    timeout rather than a clean mismatch.
    """
    path = conddb.ledger_path()
    started = clock()
    rows = conddb.read_ledger(path)
    last_change = started
    last_count = len(rows)
    log(f"Waiting for uploads to settle ({last_count} so far; quiet_for={expectations.quiet_for:.0f}s, "
        f"timeout={expectations.settle_timeout:.0f}s) -- {path}")

    while True:
        now = clock()
        if now - last_change >= expectations.quiet_for:
            return True, rows
        if now - started >= expectations.settle_timeout:
            return False, rows
        sleep(min(expectations.poll_interval, expectations.quiet_for))
        rows = conddb.read_ledger(path)
        if len(rows) != last_count:
            for row in rows[last_count:]:
                log(f"  upload: {_describe_row(row)}")
            last_count = len(rows)
            last_change = clock()


def format_report(verdict: Verdict, expectations: Optional[Expectations] = None, name="Scenario") -> str:
    """A human-readable report: what the ledger held, then what was wrong."""
    lines = []
    status = "PASSED" if verdict.ok else "FAILED"
    if not verdict.settled:
        status = "FAILED (timed out waiting for uploads to settle)"
    lines.append(f"{name} expectations: {status}")

    if expectations is not None:
        lines.append(f"  {expectations.describe()}")

    lines.append(f"  conditions DB: {len(verdict.rows)} upload(s) in {conddb.ledger_path()}")
    for row in verdict.rows:
        lines.append(f"    {_describe_row(row)}")
    if not verdict.rows:
        lines.append("    (empty)")

    if verdict.problems:
        lines.append(f"  {len(verdict.problems)} problem(s):")
        for problem in verdict.problems:
            lines.append(f"    - {problem}")
    return "\n".join(lines)
