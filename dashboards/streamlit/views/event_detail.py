"""Exact-key normalized event and original payload inspection."""

import pandas as pd
import plotly.express as px
import streamlit as st

from dashboards.streamlit import data
from dashboards.streamlit.ui import chart, details


def render() -> None:
    st.title("Event inspector")
    st.caption("Normalized record alongside original source fields")
    key = st.text_input("Exact event key", max_chars=300)
    if not key:
        st.info("Enter an event key from Quality explorer.")
        return
    try:
        rows = data.event_detail(key)
        if not rows:
            st.warning("No curated event with this key.")
            return
        row = dict(rows[0])
        raw = row.pop("raw_payload", None)
        results = data.event_quality(key)
        if results:
            checks = pd.DataFrame(results)
            checks["result"] = checks["rows_failed"].map(lambda n: "Fail" if n else "Pass")
            totals = checks.groupby(["rule_id", "result"]).size().reset_index(name="checks")
            chart(
                px.bar(
                    totals,
                    x="rule_id",
                    y="checks",
                    color="result",
                    barmode="group",
                    title="Recorded quality checks for this event · latest 100",
                )
            )
            with st.expander("Quality audit details"):
                details(results)
        normalized, source = st.columns(2)
        normalized.subheader("Curated record")
        normalized.json(row, expanded=False)
        source.subheader("Original payload")
        source.json(raw, expanded=False)
        st.caption(
            "The processed timestamp may refresh on redelivery; original payload stays unchanged."
        )
    except Exception:
        st.error("Event lookup unavailable. Check PostgreSQL and retry.")


if __name__ == "__main__":
    render()
