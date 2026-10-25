"""Run all ten scenarios against real local Kafka/PostgreSQL and OS processes."""

import json
from pathlib import Path

import pytest

from src.faults.cli import main
from src.faults.contracts import SCENARIOS
from src.faults.harness import run_scenario

pytestmark = pytest.mark.integration


@pytest.mark.parametrize("scenario_id", SCENARIOS)
def test_fault_scenario(scenario_id: str, tmp_path: Path) -> None:
    report, path = run_scenario(scenario_id, output_dir=tmp_path)
    assert report.passed, json.dumps(report.to_dict(), indent=2, default=str)
    assert main(["assert", str(path)]) == 0
    assert report.duration_seconds > 0
    if scenario_id == "consumer-crash-before-commit":
        for mode in ("before-db", "after-db"):
            assert 0 < report.evidence[mode]["recovery_duration_seconds"] < 60
    elif scenario_id == "database-outage":
        assert 0 < report.evidence["db-drop"]["recovery_duration_seconds"] < 60
