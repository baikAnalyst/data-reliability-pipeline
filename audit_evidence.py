"""Runtime transformation receipts and an independent, fail-closed state ledger.

Only explicitly instrumented operations can explain a change. A boundary diff
detects missing instrumentation; it never manufactures a removal explanation.
"""
from __future__ import annotations

from contextvars import ContextVar
from dataclasses import dataclass, field, asdict
from functools import wraps
import hashlib
import inspect
import json

import pandas as pd
import numpy as np

LINEAGE = "_dq_row_id"
ACTIVE = ContextVar("audit_evidence", default=None)


def fingerprint(frame):
    """Digest includes schema, values and row identities; order is immaterial."""
    schema = [(str(c), str(t)) for c, t in frame.dtypes.items()]
    canonical = frame.astype("string")
    for column in frame.select_dtypes(include="object"):
        canonical[column] = frame[column].map(lambda v: type(v).__qualname__).astype("string") + ":" + canonical[column].fillna("<NULL>")
    values = pd.util.hash_pandas_object(canonical, index=False)
    return hashlib.sha256(
        json.dumps(schema).encode() + np.sort(values.to_numpy()).tobytes()
    ).hexdigest()


def delta(before, after):
    for frame in (before, after):
        if LINEAGE not in frame or frame[LINEAGE].isna().any() or frame[LINEAGE].duplicated().any():
            raise ValueError("missing/null/duplicate row identity (1:N lineage NOT_AUDITED)")
    b = before.set_index(LINEAGE)
    a = after.set_index(LINEAGE)
    removed = set(b.index) - set(a.index)
    added = set(a.index) - set(b.index)
    common = b.index[b.index.isin(a.index)]
    changed, nulls = {}, {}
    for col in b.columns.intersection(a.columns):
        # The selections are in common order; comparison must stay positional.
        x = b.loc[common, col].reset_index(drop=True)
        y = a.loc[common, col].reset_index(drop=True)
        equal = x.astype("string").eq(y.astype("string")).fillna(False) | (x.isna() & y.isna())
        if x.dtype == object and y.dtype == object:
            equal &= x.map(type).eq(y.map(type)) | (x.isna() & y.isna())
        ids = tuple(common[~equal])
        if ids:
            changed[col] = ids
        ids = tuple(common[x.notna() & y.isna()])
        if ids:
            nulls[col] = ids
    schema = {
        "added": list(a.columns.difference(b.columns)),
        "removed": list(b.columns.difference(a.columns)),
        "dtype": {c: [str(b[c].dtype), str(a[c].dtype)]
                  for c in b.columns.intersection(a.columns) if b[c].dtype != a[c].dtype},
    }
    affected = removed | added | {i for ids in changed.values() for i in ids}
    if schema["added"] or schema["removed"] or schema["dtype"]:
        affected.update(common)
    return removed, added, changed, nulls, schema, affected


@dataclass
class AuditEvent:
    dataset: str
    sequence: int
    rule_id: str
    rule_type: str
    condition: dict
    action: str
    columns: tuple
    before_rows: int
    after_rows: int
    before_digest: str
    after_digest: str
    affected_ids: tuple
    removed_ids: tuple
    changed_cells: dict
    null_cells: dict
    schema: dict
    causes: dict = field(default_factory=dict)

    @property
    def affected_rows(self):
        return len(self.affected_ids)

    @property
    def removed_rows(self):
        return len(self.removed_ids)


class EvidenceLedger:
    def __init__(self, dataset, raw):
        self.dataset = dataset
        self.state = raw.copy(deep=True)
        self.events = []
        self.issues = []
        self.depth = 0

    def checkpoint(self, frame, location):
        if fingerprint(self.state) != fingerprint(frame):
            try:
                removed, added, changed, nulls, schema, affected = delta(self.state, frame)
                details = f"removed={len(removed)}, added={len(added)}, changed_cells={sum(map(len, changed.values()))}, schema={schema}"
            except ValueError as exc:
                details = str(exc)
            self.issues.append(f"NOT_AUDITED at {location}: {details}")
            self.state = frame.copy(deep=True)

    def record(self, after, rule_id, kind, condition, causes=None):
        before = self.state
        try:
            removed, added, changed, nulls, schema, affected = delta(before, after)
        except ValueError as exc:
            self.issues.append(f"NOT_AUDITED {rule_id}: {exc}")
            self.state = after.copy(deep=True)
            return
        if added:
            self.issues.append(f"unexpected new row identities in {rule_id}: {len(added)}")
        prior = {i for e in self.events for i in e.removed_ids}
        if prior & removed:
            self.issues.append(f"repeated removal identity in {rule_id}")
        cols = set(changed) | set(schema["added"]) | set(schema["removed"]) | set(schema["dtype"])
        params = condition.get("parameters", condition)
        for key in ("columns", "float_columns", "integer_columns", "keys", "ranges"):
            cols.update(params.get(key, ()))
        for key in ("column", "latitude", "longitude"):
            if key in params:
                cols.add(params[key])
        counts = {"new_null_cells:" + col: len(ids) for col, ids in nulls.items()}
        counts.update(causes or {})
        self.events.append(AuditEvent(
            self.dataset, len(self.events) + 1, rule_id, kind, condition,
            kind, tuple(sorted(cols)), len(before), len(after),
            fingerprint(before), fingerprint(after), tuple(sorted(affected)),
            tuple(sorted(removed)), changed, nulls, schema, counts,
        ))
        self.state = after.copy(deep=True)

    def validate(self, raw, final):
        """Reconcile receipts independently of the reporting/counting code."""
        try:
            actual_removed, _, _, _, _, _ = delta(raw, final)
        except ValueError as exc:
            return [str(exc)]
        issues = []
        seen = set()
        previous = fingerprint(raw)
        for position, event in enumerate(self.events, 1):
            ids = set(event.removed_ids)
            if event.sequence != position or event.dataset != self.dataset:
                issues.append("event sequence/dataset mismatch")
            if event.before_digest != previous:
                issues.append(f"broken evidence chain at {event.sequence}")
            if event.before_rows - event.after_rows != len(ids):
                issues.append(f"event row count mismatch at {event.sequence}")
            if ids - actual_removed or ids & seen:
                issues.append(f"invalid/duplicate removal evidence at {event.sequence}")
            if not ids.issubset(event.affected_ids):
                issues.append(f"affected identity mismatch at {event.sequence}")
            seen.update(ids)
            previous = event.after_digest
        if previous != fingerprint(final):
            issues.append("final state not covered by evidence chain")
        return issues


def audited_step(kind, frame_arg=0):
    """Record the actual function source/version, bound parameters and effects.

    Nested steps commit first. The outer receipt describes only subsequent
    changes, preventing a coordinate conversion being counted twice.
    """
    def decorate(fn):
        source = inspect.getsource(fn)
        signature = inspect.signature(fn)
        @wraps(fn)
        def run(*args, **kwargs):
            ledger = ACTIVE.get()
            if ledger is None:
                return fn(*args, **kwargs)
            bound = signature.bind(*args, **kwargs)
            bound.apply_defaults()
            frame = list(bound.arguments.values())[frame_arg]
            if LINEAGE not in frame and ledger.depth:
                # Local scratch frames are covered by the enclosing operation.
                token = ACTIVE.set(None)
                try:
                    return fn(*args, **kwargs)
                finally:
                    ACTIVE.reset(token)
            ledger.checkpoint(frame, fn.__name__ + ":entry")
            params = {k: v for k, v in bound.arguments.items() if not isinstance(v, (pd.DataFrame, pd.Series))}
            constants = {name: sorted(value) if isinstance(value, set) else value
                         for name, value in fn.__globals__.items()
                         if name in fn.__code__.co_names and name.isupper()
                         and isinstance(value, (dict, set, str, int, float, tuple))}
            dependencies = {name: inspect.getsource(inspect.unwrap(value))
                            for name, value in fn.__globals__.items()
                            if name in fn.__code__.co_names and inspect.isfunction(value)
                            and value.__module__ == fn.__module__}
            ledger.depth += 1
            try:
                result = fn(*args, **kwargs)
            finally:
                ledger.depth -= 1
            after = result[0] if isinstance(result, tuple) else result
            ledger.record(after, fn.__name__, kind, {
                "parameters": params, "constants": constants, "implementation": source,
                "dependencies": dependencies,
                "sha256": hashlib.sha256(source.encode()).hexdigest(),
            })
            return result
        return run
    return decorate


def audit_change(after, rule_id, kind, condition, causes=None):
    ledger = ACTIVE.get()
    if ledger is not None:
        caller = inspect.currentframe().f_back
        source = inspect.getsource(caller.f_code)
        ledger.record(after, rule_id, kind, {
            **condition, "implementation": source,
            "sha256": hashlib.sha256(source.encode()).hexdigest(),
        }, causes)


def audit_checkpoint(before, location):
    ledger = ACTIVE.get()
    if ledger is not None:
        ledger.checkpoint(before, location)
