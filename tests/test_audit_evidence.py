import json
import sys
from pathlib import Path

import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from audit_evidence import ACTIVE, audit_change, audited_step
from dataset_processors import (PROCESSORS, normalize_nulls, process_dataset,
                                convert_numeric, filter_allowed_values)
from dq_profiler import profile_processor_run, DQAuditStatus, render_dq_audit_report
from pipeline_metadata import get_dataset_specs


def run(raw, processor):
    return profile_processor_run("test", raw, processor, normalize_nulls)[1]


def test_unrecorded_removal_cannot_pass():
    r = run(pd.DataFrame({"x": [1, 2]}), lambda f: f.iloc[1:].copy())
    assert r.rows.removed_rows == 1
    assert r.rows.explained_removed_rows == 0
    assert r.rows.unexplained_row_loss == 1
    assert r.audit_checks["count_reconciliation"]
    assert not r.audit_checks["row_level_reconciliation"]
    assert r.status == DQAuditStatus.CHECK_FAILED


def test_recorded_removal_identity_and_no_double_count():
    def process(f):
        f = filter_allowed_values(f, "x", (2, 3))
        return filter_allowed_values(f, "x", (3,))
    r = run(pd.DataFrame({"x": [1, 2, 3]}), process)
    ids = [i for e in r.evidence for i in e.removed_ids]
    assert len(ids) == len(set(ids)) == r.rows.explained_removed_rows == 2
    assert {i.rsplit(":", 1)[1] for i in ids} == {"0", "1"}
    assert r.rows.unexplained_row_loss == 0
    assert all(r.audit_checks.values())
    for e in r.evidence:
        assert e.before_rows - e.after_rows == len(e.removed_ids)


def test_null_endpoints_and_stage_causes():
    def process(f):
        f = normalize_nulls(f)
        return convert_numeric(f, float_columns=("x",))
    r = run(pd.DataFrame({"x": ["-", "bad", "2", None]}), process)
    assert r.new_null_cells == 2
    assert r.new_null_causes == {"normalization": 1, "conversion": 1}
    assert r.nulls.nulls_from_normalization == 1
    assert r.nulls.nulls_from_conversion == 1
    assert r.status == DQAuditStatus.CHECK_FAILED
    assert not r.audit_checks["no_conversion_null_loss"]


def test_unknown_null_and_nonnull_mutations_fail():
    for value in (None, "changed"):
        def process(f):
            f = f.copy()
            f.loc[0, "x"] = value
            return f
        r = run(pd.DataFrame({"x": ["original"]}), process)
        assert r.status == DQAuditStatus.CHECK_FAILED
        assert any("NOT_AUDITED" in issue for issue in r.issues)
        assert r.unexpected_null_cells == (1 if value is None else 0)


def test_removed_rows_do_not_count_as_final_new_nulls():
    def process(f):
        f = normalize_nulls(f)
        return filter_allowed_values(f, "keep", (True,))
    r = run(pd.DataFrame({"x": ["-", "ok"], "keep": [False, True]}), process)
    assert r.nulls.nulls_from_normalization == 1
    assert r.new_null_cells == 0


def test_report_does_not_invent_rule_for_missing_evidence():
    r = run(pd.DataFrame({"faci_stat_nm": ["closed", "open"]}), lambda f: f.iloc[1:].copy())
    report = render_dq_audit_report(r)
    assert "UNEXPLAINED" in report
    assert "filter_allowed_values" not in report
    assert "CHECK_FAILED" in report


def raw_for(name):
    # Deliberately small offline coverage inputs; not production audit results.
    if name.startswith("culture_"):
        return pd.DataFrame({
            "FCLTY_LA": ["37.5", "999"], "FCLTY_LO": ["127", "127"],
            "ALSFC_LA": ["37.5", "999"], "ALSFC_LO": ["127", "127"],
            "CNTER_LA": ["37.5", "999"], "CNTER_LO": ["127", "127"],
            "FCLTY_CRDNT_LA": ["37.5", "999"], "FCLTY_CRDNT_LO": ["127", "127"],
            "PBTRNSP_FCLTY_LA": ["999", "37"], "PBTRNSP_FCLTY_LO": ["127", "127"],
            "MESURE_AGE_CO": ["bad", "20"], "MESURE_DE": ["bad", "20260101"],
            "BASE_YEAR": ["2026", "2026"], "FCLTY_NM": ["-", "name"],
            "CTPRVN_NM": ["서울", "서울"],
        })
    cases = {
        "facility": {"faci_stat_nm": ["정상운영", "폐업"], "faci_lat": ["37", "37"], "faci_lot": ["127", "127"], "faci_cd": ["A", "B"]},
        "aed": {"serialSeq": ["A", "A"], "wgs84Lat": ["37", "37"], "wgs84Lon": ["127", "127"]},
        "public_open_facility": {"latitude": ["37", "999"], "longitude": ["127", "127"], "rdnmadr": ["서울 중구", "-"]},
        "air_quality": {"sidoName": ["서울", "서울"], "stationName": ["A", "A"], "dataTime": ["2026-01-01 12:00", "2026-01-01 12:00"], "pm10Value": ["-", "bad"]},
        "weather_ultra_ncst": {"baseDate": ["20260101"], "nx": ["60"], "ny": ["127"], "category": ["T1H"], "obsrValue": ["bad"]},
        "weather_ultra_fcst": {"baseDate": ["20260101"], "category": ["RN1"], "fcstValue": ["강수없음"]},
        "weather_warning": {"stnId": ["108", "108"], "tmFc": ["202601011200", "202601011200"], "tmSeq": ["1", "1"]},
        "weather_warning_status": {"t6": ["없음"], "t7": ["없음"], "tmFc": ["202601011200"], "tmSeq": ["1"]},
        "durunubi_trails": {"routeIdx": ["A"], "themeNm": ["-"]},
        "durunubi_segments": {"routeIdx": ["A"], "crsIdx": ["B"], "crsDstnc": ["bad"]},
        "koroad_bicycle_accident_hotspots": {k: [v] for k, v in {
            "afos_fid": "A", "afos_id": "1", "expected_afos_id": "1", "bjd_cd": "1", "spot_cd": "1", "sido_sgg_nm": "서울", "spot_nm": "name",
            "request_year": "2024", "request_sido": "11", "request_gugun": "110", "request_page_no": "1", "lo_crd": "127", "la_crd": "37",
            "geom_json": '{"type":"Polygon","coordinates":[[[127,37],[128,37],[127,37]]]}', "collected_at": "2026-01-01T00:00:00+09:00",
        }.items()},
    }
    return pd.DataFrame(cases[name])


@pytest.mark.parametrize("name", sorted(PROCESSORS))
def test_every_registered_processor_runtime_coverage(name):
    raw = raw_for(name)
    expected = process_dataset(name, raw, allow_empty=True)
    actual, r = profile_processor_run(name, raw, lambda f: process_dataset(name, f, allow_empty=True), normalize_nulls)
    pd.testing.assert_frame_equal(actual, expected)
    assert r.evidence
    assert r.audit_checks["transformation_evidence"], r.issues
    assert r.audit_checks["internal_consistency"], r.issues
    expected_status = DQAuditStatus.CHECK_FAILED if r.nulls.nulls_from_conversion else DQAuditStatus.CHECK_PASSED
    assert r.status == expected_status, r.issues
    assert r.rows.unexplained_row_loss == 0
    assert r.unexpected_null_cells == 0
    json.dumps(r.as_dict(), ensure_ascii=False)


def test_metadata_processor_coverage_is_exhaustive():
    registered = {t.processor for s in get_dataset_specs().values() for t in s.tables}
    assert registered == set(PROCESSORS)


def test_one_to_many_is_explicitly_not_audited():
    raw = pd.DataFrame({"t6": ["강풍주의보: 서울특별시\n호우경보: 인천광역시"], "t7": ["없음"], "tmFc": ["202601011200"], "tmSeq": ["1"]})
    r = run(raw, lambda f: process_dataset("weather_warning_status", f, allow_empty=True))
    assert r.status == DQAuditStatus.CHECK_FAILED
    assert any("NOT_AUDITED" in s for s in r.issues)


def test_tampered_receipt_fails_independent_verification():
    def process(f):
        f = filter_allowed_values(f, "x", (2,))
        ACTIVE.get().events[0].before_rows += 1
        return f
    r = run(pd.DataFrame({"x": [1, 2]}), process)
    assert not r.audit_checks["internal_consistency"]
    assert r.status == DQAuditStatus.CHECK_FAILED


def test_legacy_reason_claim_is_not_runtime_removal_evidence():
    from dq_profiler import record_removal_reason
    def process(f):
        record_removal_reason(f.iloc[:1], "invented")
        return f.iloc[1:].copy()
    r = run(pd.DataFrame({"x": [1, 2]}), process)
    assert r.rows.explained_removed_rows == 0
    assert r.status == DQAuditStatus.CHECK_FAILED


def test_rename_retains_null_transition_identity():
    def process(f):
        f = f.rename(columns={"old": "new"})
        audit_change(f, "rename", "rename", {"mapping": {"old": "new"}})
        return convert_numeric(f, float_columns=("new",))
    r = run(pd.DataFrame({"old": ["bad"]}), process)
    assert r.new_null_cells == 1
    assert r.new_null_causes == {"conversion": 1}


def test_normalization_before_rename_keeps_its_cause():
    def process(f):
        f = normalize_nulls(f)
        f = f.rename(columns={"old": "new"})
        audit_change(f, "rename", "rename", {"mapping": {"old": "new"}})
        return f
    r = run(pd.DataFrame({"old": ["-"]}), process)
    assert r.new_null_cells == 1
    assert r.new_null_causes == {"normalization": 1}


def test_intermediate_unrecorded_change_before_helper_is_detected():
    def process(f):
        f = f.copy()
        f["x"] = "unrecorded"
        return normalize_nulls(f)
    r = run(pd.DataFrame({"x": ["raw"]}), process)
    assert not r.audit_checks["transformation_evidence"]
    assert r.status == DQAuditStatus.CHECK_FAILED


def test_object_value_type_change_cannot_hide_behind_same_text():
    def process(f):
        f = f.copy()
        f.loc[0, "x"] = "1"
        return f
    r = run(pd.DataFrame({"x": pd.Series([1, "other"], dtype=object)}), process)
    assert r.status == DQAuditStatus.CHECK_FAILED


def test_normalization_is_not_invented_when_processor_does_not_run_it():
    r = run(pd.DataFrame({"x": ["-"]}), lambda f: f.copy())
    assert r.nulls.nulls_from_normalization == 0
    assert r.new_null_cells == 0


def test_common_filter_and_dedup_record_without_optional_reason():
    from dataset_processors import filter_korea_coordinates, deduplicate_valid_keys
    def process(f):
        f = filter_korea_coordinates(f, "lat", "lon")
        return deduplicate_valid_keys(f, ("key",))
    r = run(pd.DataFrame({"lat": [37, 37, 999], "lon": [127, 127, 127], "key": ["a", "a", "b"]}), process)
    assert r.rows.explained_removed_rows == 2
    assert r.dedup_removed == r.dedup.removed_rows == 1
    assert r.status == DQAuditStatus.CHECK_PASSED


def test_optional_coordinate_invalid_pair_has_evidence():
    from dataset_processors import clean_optional_coordinates
    r = run(pd.DataFrame({"lat": [None], "lon": [127.0]}),
            lambda f: clean_optional_coordinates(f, "lat", "lon"))
    assert r.new_null_cells == 1
    assert r.new_null_causes == {"invalid_value": 1}
    assert r.status == DQAuditStatus.CHECK_PASSED


def test_empty_frame_remains_auditable():
    r = run(pd.DataFrame({"x": pd.Series([], dtype="string")}), normalize_nulls)
    assert r.status == DQAuditStatus.CHECK_PASSED
    assert r.new_null_cells == 0


def test_unknown_or_duplicate_identity_fails():
    for mode in ("unknown", "duplicate"):
        def process(f):
            f = f.copy()
            f.loc[0, "_dq_row_id"] = "unknown" if mode == "unknown" else f.loc[1, "_dq_row_id"]
            return f
        r = run(pd.DataFrame({"x": [1, 2]}), process)
        assert not r.audit_checks["row_lineage"]
        assert r.status == DQAuditStatus.CHECK_FAILED


@pytest.mark.parametrize("tracked,unexplained", [(2, 0), (1, 1)])
def test_ten_to_eight_runtime_reconciliation(tracked, unexplained):
    def process(f):
        f = filter_allowed_values(f, "x", tuple(range(tracked, 10)))
        return f.loc[f.x >= 2].copy()
    r = run(pd.DataFrame({"x": range(10)}), process)
    assert (r.rows.raw_rows, r.rows.processed_rows) == (10, 8)
    assert r.rows.explained_removed_rows == tracked
    assert r.rows.unexplained_row_loss == unexplained
    assert (r.status == DQAuditStatus.CHECK_PASSED) == (unexplained == 0)


def test_overlapping_reasons_count_unique_removed_row():
    from dq_profiler import add_stable_lineage, reconcile_row_counts
    raw = add_stable_lineage(pd.DataFrame({"x": range(10)}))
    row_id = raw.iloc[0]["_dq_row_id"]
    r = reconcile_row_counts(raw, raw.iloc[1:], removal_reasons={"a": [row_id], "b": [row_id]})
    assert r.explained_removed_rows == 1
    assert r.unexplained_row_loss == 0


def test_no_evidence_no_changes_cannot_pass():
    r = run(pd.DataFrame({"x": [None]}), lambda f: f.copy())
    assert not r.evidence
    assert r.status == DQAuditStatus.CHECK_FAILED


def test_null_identity_survives_string_index_and_reordering():
    # IDs :10 and :2 sort differently lexically; normalization changes index dtype.
    raw = pd.DataFrame({"x": ["2"] * 12})
    raw.loc[10, "x"] = "-"
    raw.loc[11, "x"] = "bad"
    def process(f):
        f = convert_numeric(normalize_nulls(f), float_columns=("x",))
        return f.iloc[::-1].reset_index(drop=True)
    r = run(raw, process)
    assert r.new_null_causes == {"normalization": 1, "conversion": 1}
    assert r.unexpected_null_cells == 0
    assert r.new_null_cells == 2
    assert r.status == DQAuditStatus.CHECK_FAILED


def test_inplace_conversion_preserves_raw_audit_baseline():
    raw = pd.DataFrame({"x": ["bad", None]})
    r = run(raw, lambda f: convert_numeric(f, float_columns=("x",)))
    assert raw.iloc[0].x == "bad"
    assert r.nulls.source_null == 1
    assert r.new_null_cells == 1
    assert r.new_null_causes == {"conversion": 1}


def test_stored_snapshot_values_and_nulls_are_verified():
    from dq_profiler import audit_stored_processed
    raw = pd.DataFrame({"id": ["a", "b"], "x": ["-", "ok"]})
    replay, r = profile_processor_run("test", raw, normalize_nulls, normalize_nulls)
    stored = pd.DataFrame({"id": ["b", "a"], "x": ["ok", None]})
    verified = audit_stored_processed(r, replay, stored, ("id",))
    assert verified.status == DQAuditStatus.CHECK_PASSED
    stored.loc[0, "x"] = None
    failed = audit_stored_processed(r, replay, stored, ("id",))
    assert failed.status == DQAuditStatus.CHECK_FAILED
    assert "stored PROCESSED mismatch: x=1 cells" in failed.issues


@pytest.mark.parametrize("mode", ["missing_key", "duplicate_key", "row_count", "schema"])
def test_stored_snapshot_cannot_pass_without_comparable_identity(mode):
    from dq_profiler import audit_stored_processed
    raw = pd.DataFrame({"id": ["a", "b"], "x": ["1", "2"]})
    replay, r = profile_processor_run("test", raw, normalize_nulls, normalize_nulls)
    stored = replay.copy()
    keys = ("id",)
    if mode == "missing_key":
        keys = ()
    elif mode == "duplicate_key":
        stored["id"] = "a"
    elif mode == "row_count":
        stored = stored.iloc[:1]
    else:
        stored = stored.drop(columns="x")
    verified = audit_stored_processed(r, replay, stored, keys)
    assert not verified.audit_checks["stored_processed_matches_replay"]
    assert verified.status == DQAuditStatus.CHECK_FAILED


@pytest.mark.parametrize("sentinel", [None, pd.NA, float("nan"), pd.NaT])
def test_stored_null_sentinels_do_not_enter_value_comparison(sentinel):
    from dq_profiler import audit_stored_processed
    raw = pd.DataFrame({"id": [str(i) for i in range(12)], "x": ["ok"] * 11 + ["-"]})
    replay, r = profile_processor_run("test", raw, normalize_nulls, normalize_nulls)
    stored = replay.astype(object)
    stored.loc[11, "x"] = sentinel
    assert audit_stored_processed(r, replay, stored, ("id",)).audit_checks["stored_processed_matches_replay"]
    stored.loc[10, "x"] = sentinel
    assert not audit_stored_processed(r, replay, stored, ("id",)).audit_checks["stored_processed_matches_replay"]
