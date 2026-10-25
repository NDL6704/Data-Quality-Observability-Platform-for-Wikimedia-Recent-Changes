"""Acceptance contracts shared by the public README, schema and persistence."""

import json
from datetime import UTC, datetime
from pathlib import Path

from src.quality.engine import QualityEngine, RuleSeverity
from src.quality.rules import build_default_rule_set


def test_catalog_has_ten_prechecks_and_one_persistence_check() -> None:
    rules = build_default_rule_set()
    expected = {
        "R-SCH-01",
        "R-SCH-02",
        "R-CMP-01",
        "R-CMP-02",
        "R-VAL-01",
        "R-VAL-02",
        "R-VAL-03",
        "R-DED-01",
        "R-FRS-01",
        "R-DRF-01",
    }
    assert {r.rule_id for r in rules} == expected
    assert len(rules) == 10 and len(expected | {"R-DED-02"}) == 11
    assert {r.rule_id: r.rule_version for r in rules} == {
        rule_id: "1.2.0" if rule_id == "R-FRS-01" else "1.1.0" for rule_id in expected
    }
    by_id = {r.rule_id: r for r in rules}
    assert by_id["R-SCH-02"].severity == RuleSeverity.CRITICAL
    assert by_id["R-DED-01"].severity == RuleSeverity.INFO


def test_raw_schema_allows_optional_ids_unknown_types_and_namespace_normalization() -> None:
    schema = json.loads(Path("schemas/wikimedia_recentchange_v1.json").read_text(encoding="utf-8"))
    assert set(schema["required"]) == {"meta", "wiki", "type"}
    assert set(schema["properties"]["meta"]["required"]) == {"dt", "stream"}
    assert "enum" not in schema["properties"]["type"]
    assert "type" not in schema["properties"]["namespace"]
    assert "null" in schema["properties"]["meta"]["properties"]["id"]["type"]


def test_readme_catalog_and_code_ids_agree() -> None:
    readme = Path("README.md").read_text(encoding="utf-8")
    table = readme.split("<!-- rule-catalog:start -->", 1)[1].split(
        "<!-- rule-catalog:end -->", 1
    )[0]
    ids = {r.rule_id for r in build_default_rule_set()} | {"R-DED-02"}
    for rule_id in ids:
        version = "1.2.0" if rule_id == "R-FRS-01" else "1.1.0"
        assert f"| `{rule_id}` | `{version}` |" in table


def test_normalized_handoff_accepts_fallback_and_unknown_type() -> None:
    now = datetime.now(UTC)
    payload = {
        "meta": {"dt": now.isoformat(), "stream": "mediawiki.recentchange"},
        "type": "future_type",
        "wiki": "viwiki",
        "server_name": "vi.wikipedia.org",
        "namespace": "abc",
    }
    engine = QualityEngine()
    engine.register_rules(build_default_rule_set())
    prepared = engine.prepare_payload(payload, received_at=now)
    assert not prepared.summary.should_quarantine
    assert {"WARN_WEAK_EVENT_KEY", "WARN_UNKNOWN_EVENT_TYPE", "ERR_INVALID_NAMESPACE"} <= set(
        prepared.summary.failure_codes
    )
    assert prepared.to_curated_record().namespace is None
    assert prepared.to_curated_record().event_key == prepared.summary.event_key
