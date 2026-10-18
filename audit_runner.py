"""Read-only audit of existing RAW data. Never collects or loads PROCESSED."""
from __future__ import annotations

import argparse
import json
from pathlib import Path
import time
import sys

import pandas as pd
from sqlalchemy import text

from dataset_processors import normalize_nulls, process_dataset
from dq_profiler import profile_processor_run, render_dq_audit_report, audit_stored_processed
from pipeline_metadata import get_dataset_specs


def registered_tables():
    return {t.table: t for spec in get_dataset_specs().values() for t in spec.tables}


def main(argv=None):
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")
    tables = registered_tables()
    parser = argparse.ArgumentParser(description=__doc__)
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--table", choices=sorted(tables))
    group.add_argument("--all", action="store_true")
    group.add_argument("--list", action="store_true")
    parser.add_argument("--csv", type=Path, help="Existing RAW CSV; requires --table")
    parser.add_argument("--key", action="append", help="Stored comparison key; repeat for composite keys")
    parser.add_argument("--output-dir", type=Path, help="Save full evidence JSON and text locally")
    args = parser.parse_args(argv)
    if args.csv and not args.table:
        parser.error("--csv requires --table")
    if args.list:
        print("\n".join(sorted(tables)))
        return 0
    engine = None
    failures = []
    try:
        if not args.csv:
            from pipeline_elt import create_db_engine
            engine = create_db_engine()
        for name in ([args.table] if args.table else sorted(tables)):
            spec = tables[name]
            start = time.perf_counter()
            print(f"[{name}] reading existing RAW", flush=True)
            try:
                if args.csv:
                    raw = pd.read_csv(args.csv, dtype="string", keep_default_na=False)
                else:
                    with engine.connect() as connection:
                        with connection.begin():
                            connection.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY"))
                            raw = pd.read_sql_query(text(f'SELECT * FROM "raw"."{name}"'), connection)
                            stored = pd.read_sql_query(text(f'SELECT * FROM "processed"."{name}"'), connection)
                print(f"[{name}] {len(raw):,} rows / {len(raw.columns)} columns; read {time.perf_counter()-start:.1f}s; auditing", flush=True)
                replay, result = profile_processor_run(
                    spec.processor, raw,
                    lambda frame: process_dataset(spec.processor, frame, allow_empty=spec.allow_empty),
                    normalize_nulls, dedup_keys=spec.upsert_key or spec.primary_key,
                )
                if not args.csv:
                    result = audit_stored_processed(result, replay, stored, args.key or spec.upsert_key or spec.primary_key)
                    print(f"DB snapshot RAW={len(raw):,} PROCESSED={len(stored):,} REMOVED={len(raw)-len(stored):,}")
                print("Scope: current transformation replay; historical runtime evidence NOT_AUDITED")
                report = render_dq_audit_report(result)
                payload = result.as_dict()
                print(report, flush=True)
                if result.status.value != "CHECK_PASSED":
                    failures.append(name)
            except Exception as exc:
                failures.append(name)
                payload = {"dataset": name, "status": "CHECK_FAILED", "audit_complete": False,
                           "error": f"{type(exc).__name__}: {exc}"}
                report = json.dumps(payload, ensure_ascii=False, indent=2)
                print(report, flush=True)
            if args.output_dir:
                args.output_dir.mkdir(parents=True, exist_ok=True)
                (args.output_dir / f"{name}.json").write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
                (args.output_dir / f"{name}.txt").write_text(report, encoding="utf-8")
            print(f"[{name}] elapsed {time.perf_counter()-start:.1f}s", flush=True)
    finally:
        if engine is not None:
            engine.dispose()
    print("Audit failures: " + (", ".join(failures) if failures else "none"))
    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
