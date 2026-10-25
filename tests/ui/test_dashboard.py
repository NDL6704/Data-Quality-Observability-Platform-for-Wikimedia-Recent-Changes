"""AppTest validates rendered states and explicit actions, including real DB replay."""

from collections.abc import Generator
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from unittest.mock import patch
from uuid import uuid4

import pytest
from streamlit.testing.v1 import AppTest

from src.faults.scenarios import StrictTitleRule
from src.quality.engine import QualityEngine
from src.quality.rules import build_default_rule_set
from src.storage.db import get_db_connection
from src.storage.repository import QualityRepository

ROOT = Path(__file__).resolve().parents[2]


@pytest.fixture(autouse=True)
def auxiliary_chart_data() -> Generator[None, None, None]:
    """Control secondary charts; primary replay/pool tests still use actual DB."""
    with (
        patch("dashboards.streamlit.data.quality_trend", return_value=[]),
        patch("dashboards.streamlit.data.wiki_distribution", return_value=[]),
        patch("dashboards.streamlit.data.event_quality", return_value=[]),
        patch("dashboards.streamlit.data.quarantine_summary", return_value=[]),
    ):
        yield


def test_shell_and_overview_navigation() -> None:
    with (
        patch(
            "dashboards.streamlit.data.overview",
            return_value={"curated": 2, "pending": 1, "incidents": 0, "corrupted": 0},
        ),
        patch("dashboards.streamlit.data.incidents", return_value=[]),
        patch("dashboards.streamlit.data.prom_query", return_value=[]),
    ):
        app = AppTest.from_file(ROOT / "dashboards/streamlit/app.py").run(timeout=20)
        assert not app.exception
        assert len(app.metric) == 4 and app.metric[0].value == "2"
        assert app.title[0].value == "Reliability & lag"


def test_outage_is_unknown_not_healthy_zero() -> None:
    with (
        patch("dashboards.streamlit.data.overview", side_effect=OSError("injected")),
        patch("dashboards.streamlit.data.incidents", return_value=[]),
        patch("dashboards.streamlit.data.prom_query", side_effect=OSError("injected")),
    ):
        app = AppTest.from_file(ROOT / "dashboards/streamlit/views/overview.py").run(timeout=20)
        assert not app.exception and len(app.metric) == 0
        assert "unknown" in app.error[0].value
        assert "unknown" in app.warning[0].value


def test_quality_filters_and_modal() -> None:
    rows = [{"event_key": "test:key", "wiki": "viwiki", "title": "Test"}]
    with (
        patch(
            "dashboards.streamlit.data.quality",
            return_value=[{"rule_id": "R-CMP-02", "failure_rate": 0.5}],
        ) as rules,
        patch("dashboards.streamlit.data.curated", return_value=rows) as events,
        patch("dashboards.streamlit.data.event_detail", return_value=rows),
    ):
        app = AppTest.from_file(ROOT / "dashboards/streamlit/views/quality_explorer.py").run(
            timeout=20
        )
        app.selectbox[0].select(6).run()
        app.text_input[2].set_value("viwiki").run()
        rules.assert_called_with(6, "", "", None, None)
        events.assert_called_with("viwiki", "")
        app.button[0].click().run()
        assert not app.exception and len(app.json) == 1


def test_quality_versions_are_grouped_not_stacked() -> None:
    import json

    with (
        patch(
            "dashboards.streamlit.data.quality",
            return_value=[
                {
                    "rule_id": "R-FRS-01",
                    "rule_version": "1.1.0",
                    "checked": 10,
                    "failed": 10,
                    "failure_rate": 1.0,
                },
                {
                    "rule_id": "R-FRS-01",
                    "rule_version": "1.2.0",
                    "checked": 10,
                    "failed": 5,
                    "failure_rate": 0.5,
                },
            ],
        ),
        patch("dashboards.streamlit.data.curated", return_value=[]),
    ):
        app = AppTest.from_file(ROOT / "dashboards/streamlit/views/quality_explorer.py").run()
        assert not app.exception
        spec = json.loads(app.get("plotly_chart")[0].proto.spec)
        assert spec["layout"]["barmode"] == "group"
        assert spec["layout"]["xaxis"]["range"] == [0, 1]
        assert all(trace["orientation"] == "h" for trace in spec["data"])
        assert app.metric[2].value == "75.0%"


def test_quality_rejects_timezone_ambiguous_filter() -> None:
    with (
        patch("dashboards.streamlit.data.quality", return_value=[]) as query,
        patch("dashboards.streamlit.data.curated", return_value=[]),
    ):
        app = AppTest.from_file(ROOT / "dashboards/streamlit/views/quality_explorer.py").run()
        query.reset_mock()
        app.text_input[0].set_value("2026-10-25T15:48:46").run()
        query.assert_not_called()
        assert app.error and "timezone" in app.error[0].value


def test_event_original_and_normalized() -> None:
    with patch(
        "dashboards.streamlit.data.event_detail",
        return_value=[
            {"event_key": "test:key", "namespace": None, "raw_payload": {"namespace": "abc"}}
        ],
    ) as query:
        app = AppTest.from_file(ROOT / "dashboards/streamlit/views/event_detail.py").run(timeout=20)
        app.text_input[0].set_value("test:key").run()
        query.assert_called_with("test:key")
        assert not app.exception and len(app.json) == 2


def test_incident_investigation_requires_note() -> None:
    row = {"incident_id": "testid", "status": "OPEN", "incident_type": "test"}
    with (
        patch("dashboards.streamlit.data.incidents", return_value=[row]),
        patch("src.observability.incidents.IncidentManager") as cls,
    ):
        cls.return_value.investigate.return_value = True
        app = AppTest.from_file(ROOT / "dashboards/streamlit/views/incidents.py").run(timeout=20)
        app.button[0].click().run()
        cls.return_value.investigate.assert_not_called()
        app.text_area[0].set_value("Investigating source reconnects")
        app.button[0].click().run()
        cls.return_value.investigate.assert_called_once_with(
            "testid", "Investigating source reconnects"
        )
        assert not app.exception and app.success


@pytest.fixture
def pending_replay() -> Generator[dict[str, Any], None, None]:
    raw = {
        "meta": {
            "id": str(uuid4()),
            "dt": datetime.now(UTC).isoformat(),
            "stream": "mediawiki.recentchange",
        },
        "wiki": "ui_test",
        "type": "edit",
        "server_name": "example.invalid",
    }
    engine = QualityEngine()
    engine.register_rules([*build_default_rule_set(), StrictTitleRule()])
    prepared = engine.prepare_payload(raw)
    with get_db_connection() as conn:
        outcome = QualityRepository(conn).persist_prepared(prepared)
    state = {"id": outcome.quarantine_id, "key": prepared.summary.event_key}
    try:
        yield state
    finally:
        with get_db_connection() as conn:
            row = conn.execute(
                "SELECT replay_run_id FROM quarantined_events WHERE id=%s", (state["id"],)
            ).fetchone()
            conn.execute(
                "DELETE FROM quality_results WHERE failure_details->0->>'event_key'=%s",
                (state["key"],),
            )
            conn.execute("DELETE FROM curated_changes WHERE event_key=%s", (state["key"],))
            conn.execute("DELETE FROM quarantined_events WHERE id=%s", (state["id"],))
            if row and row["replay_run_id"]:
                conn.execute("DELETE FROM pipeline_runs WHERE run_id=%s", (row["replay_run_id"],))


@pytest.mark.integration
def test_real_preview_readonly_and_confirmed_replay(pending_replay: dict[str, Any]) -> None:
    state = pending_replay
    app = AppTest.from_file(ROOT / "dashboards/streamlit/views/replay_console.py").run(timeout=20)
    app.text_input[1].set_value(state["key"]).run()
    assert not app.exception
    with get_db_connection() as conn:
        assert (
            conn.execute(
                "SELECT 1 FROM curated_changes WHERE event_key=%s", (state["key"],)
            ).fetchone()
            is None
        )
    app.button[0].click().run()
    assert "confirmation" in app.warning[-1].value
    app.checkbox[0].check()
    app.button[0].click().run()
    assert not app.exception and app.success
    with get_db_connection() as conn:
        row = conn.execute(
            "SELECT replay_status FROM quarantined_events WHERE id=%s", (state["id"],)
        ).fetchone()
        assert row and row["replay_status"] == "REPLAYED"
        assert conn.execute(
            "SELECT 1 FROM curated_changes WHERE event_key=%s", (state["key"],)
        ).fetchone()


@pytest.mark.integration
def test_pool_rejects_accidental_writes() -> None:
    import psycopg

    from dashboards.streamlit.data import read

    with pytest.raises(psycopg.errors.ReadOnlySqlTransaction):
        read("CREATE TEMP TABLE ui_should_not_write (id integer)")
    assert read("SELECT 1 AS healthy")[0]["healthy"] == 1
