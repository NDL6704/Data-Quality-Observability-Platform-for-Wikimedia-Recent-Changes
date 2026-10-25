"""Version-aware quality charts with explicit evaluation and receipt cohorts."""

import pandas as pd
import plotly.express as px
import streamlit as st

from dashboards.streamlit import data
from dashboards.streamlit.ui import chart, details, parse_instant
from src.observability.metrics import RULE_IDS


def render() -> None:
    st.title("Quality explorer")
    st.caption("Understand the rule, version and arrival cohort behind each result")
    window, left, right = st.columns(3)
    hours = window.selectbox("Evaluation window (hours)", [1, 6, 24, 168], index=2)
    rule = left.selectbox("Rule", ["ALL", *sorted(RULE_IDS)])
    version = right.selectbox("Rule version", ["ALL", "1.2.0", "1.1.0"])
    with st.expander("Precise time filters · ISO 8601 with timezone"):
        since_text = st.text_input("Evaluated since", placeholder="2026-10-25T08:48:46Z")
        receipt_text = st.text_input(
            "Original receipt since (freshness only)", placeholder="2026-10-25T08:48:46Z"
        )
        st.caption(
            "Receipt filter selects R-FRS-01 only. Old input processed today stays old input."
        )
    try:
        since, receipt = parse_instant(since_text), parse_instant(receipt_text)
    except ValueError as exc:
        st.error(str(exc))
        return
    selected = "R-FRS-01" if receipt else ("" if rule == "ALL" else rule)
    args = (hours, selected, "" if version == "ALL" else version, since, receipt)
    try:
        rules = data.quality(*args)
        if rules:
            checked = sum(int(r.get("checked", 0)) for r in rules)
            failed = sum(int(r.get("failed", 0)) for r in rules)
            a, b, c = st.columns(3)
            a.metric("Rule checks", f"{checked:,}")
            b.metric("Failed checks", f"{failed:,}")
            c.metric("Weighted failure rate", f"{failed / checked:.1%}" if checked else "Unknown")
            frame = pd.DataFrame(rules)
            frame["rule_version"] = frame.get("rule_version", "unknown")
            frame["failure_rate"] = frame["failure_rate"].astype(float)
            fig = px.bar(
                frame,
                x="failure_rate",
                y="rule_id",
                orientation="h",
                color="rule_version",
                barmode="group",
                title="Failure rate by rule and version",
            )
            upper = min(1.0, max(0.01, float(frame["failure_rate"].max()) * 1.15))
            fig.update_xaxes(range=[0, upper], tickformat=".1%")
            comparison, history, audit = st.tabs(["By rule", "Trend", "Audit totals"])
            with comparison:
                chart(fig)
            trend = data.quality_trend(*args)
            if trend:
                timeline = pd.DataFrame(trend).sort_values("time")
                fig = px.line(
                    timeline,
                    x="time",
                    y="failure_rate",
                    markers=True,
                    title="Failure trend · weighted checks per time bucket",
                )
                fig.update_yaxes(range=[0, 1], tickformat=".0%")
                with history:
                    chart(fig)
            else:
                history.info("No trend samples match these filters.")
            with audit:
                details(rules)
        else:
            st.info("No evaluations match these filters.")
        st.caption(
            "Attempt-weighted ratios, never added across versions. Window uses evaluation time."
        )
        with st.expander("Find and inspect an event"):
            st.subheader("Find curated events")
            wiki = st.text_input("Wiki (exact, optional)", max_chars=100)
            title = st.text_input("Title contains", max_chars=200)
            rows = data.curated(wiki, title)
            if rows:
                key = st.selectbox("Event to inspect", [row["event_key"] for row in rows])
                if st.button("Inspect event"):
                    show_event(key)
                with st.expander("Search results · up to 100 records"):
                    details(rows)
            else:
                st.info("No curated events match this search.")
    except Exception:
        st.error("Quality data unavailable. Check PostgreSQL and retry.")


@st.dialog("Event detail")
def show_event(key: str) -> None:
    rows = data.event_detail(key)
    if rows:
        st.json(rows[0], expanded=False)
    else:
        st.info("Event no longer available.")


if __name__ == "__main__":
    render()
