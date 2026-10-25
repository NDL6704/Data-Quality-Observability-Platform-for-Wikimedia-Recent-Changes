"""Verify the CLI never treats absent or failed evidence as a passing run."""

import json
from pathlib import Path

import pytest

from src.faults.cli import main
from src.faults.contracts import REQUIRED_CHECKS, SCENARIOS, ScenarioReport


def test_list_is_offline(capsys: pytest.CaptureFixture[str]) -> None:
    assert main(["list"]) == 0
    assert len(capsys.readouterr().out.splitlines()) == len(SCENARIOS) == 10


@pytest.mark.parametrize("checks", [{}, {"recovered": False}, {"recovered": 1}])
def test_report_rejects_missing_or_untrue_checks(checks: dict[str, bool]) -> None:
    assert not ScenarioReport("duplicate-delivery", "test", checks=checks).passed


def test_assert_recomputes_outcome(tmp_path: Path) -> None:
    path = tmp_path / "report.json"
    data = ScenarioReport("duplicate-delivery", "test", checks={"unique": False}).to_dict()
    data["passed"] = True
    path.write_text(json.dumps(data), encoding="utf-8")
    assert main(["assert", str(path)]) == 1
    data["checks"] = dict.fromkeys(REQUIRED_CHECKS["duplicate-delivery"], True)
    path.write_text(json.dumps(data), encoding="utf-8")
    assert main(["assert", str(path)]) == 0


def test_invalid_report(tmp_path: Path) -> None:
    path = tmp_path / "bad.json"
    path.write_text("{}", encoding="utf-8")
    assert main(["assert", str(path)]) == 2


def test_incomplete_report_never_passes() -> None:
    assert not ScenarioReport("duplicate-delivery", "test", checks={"curated_unique": True}).passed
