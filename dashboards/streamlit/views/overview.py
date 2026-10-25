"""Reliability cards, measured lag history and incident ticker."""

from datetime import UTC, datetime

import pandas as pd
import plotly.express as px
import streamlit as st

from dashboards.streamlit import data
from dashboards.streamlit.ui import chart, details


def render() -> None:
    st.title("Reliability & lag")
    st.caption("Durable outcomes, telemetry health and recent incidents")
    st.button("Refresh", key="refresh_overview")
    try:
        counts = data.overview()
        columns = st.columns(4)
        for column, label, field in zip(
            columns,
            ("Curated events", "Pending quarantine", "Active incidents", "Corrupt archives"),
            ("curated", "pending", "incidents", "corrupted"),
            strict=True,
        ):
            column.metric(label, int(counts[field]))
        st.caption("Independent counts, not a data-loss balance.")
    except Exception:
        st.error("Database unavailable. Counts are unknown; check the database connection.")
    backlog, throughput, distribution_tab, incidents_tab = st.tabs(
        ["Backlog", "Throughput", "Wikis", "Incidents"]
    )
    try:
        series = data.prom_query("sum by (topic) (wikimedia_consumer_lag_records)", history=True)
        points = [
            {
                "time": datetime.fromtimestamp(float(t), UTC),
                "topic": row["metric"].get("topic", "raw"),
                "records": float(v),
            }
            for row in series
            for t, v in row.get("values", [])
        ]
        if points:
            with backlog:
                chart(
                    px.area(
                        pd.DataFrame(points),
                        x="time",
                        y="records",
                        color="topic",
                        title="Unacknowledged Kafka deliveries",
                    )
                )
        else:
            backlog.info("No lag samples. Start the observability collector for live telemetry.")
        targets = data.prom_query('up{job=~"ingestor|quality|observability"}')
        st.caption(
            "  ·  ".join(
                f"{r['metric'].get('job')}: {'UP' if float(r['value'][1]) else 'DOWN'}"
                for r in targets
            )
        )
        rates = data.prom_query(
            "sum by (job) (rate(wikimedia_ingest_events_total[2m])) or "
            "sum by (job) (rate(wikimedia_consumer_throughput_total[2m]))",
            history=True,
        )
        rate_points = [
            {
                "time": datetime.fromtimestamp(float(t), UTC),
                "service": r["metric"].get("job", "unknown"),
                "events_per_second": float(v),
            }
            for r in rates
            for t, v in r.get("values", [])
        ]
        if rate_points:
            with throughput:
                chart(
                    px.line(
                        pd.DataFrame(rate_points),
                        x="time",
                        y="events_per_second",
                        color="service",
                        title="Input vs acknowledged processing rate",
                    )
                )
        backlog.caption("Last 30 minutes. Stopped exporters can leave historical samples.")
        probes = data.prom_query("wikimedia_observability_probe_success")
        if any(float(r["value"][1]) == 0 for r in probes):
            st.warning("A state probe is failing. Last-good lag may be stale.")
    except Exception:
        st.warning("Prometheus unavailable. Lag and telemetry health are unknown.")

    try:
        rows = data.incidents("ALL")
        with incidents_tab:
            for row in rows[:5]:
                st.info(
                    f"{row.get('status')} · {row.get('incident_type')} · {row.get('severity', '')}"
                )
            with st.expander("Recent incident details"):
                details(rows[:10])
        distribution = data.wiki_distribution()
        if distribution:
            with distribution_tab:
                chart(
                    px.bar(
                        pd.DataFrame(distribution),
                        x="events",
                        y="wiki",
                        orientation="h",
                        title="Curated events by wiki · top 12",
                    )
                )
    except Exception:
        st.info("Incident ticker unavailable.")


if __name__ == "__main__":
    render()
