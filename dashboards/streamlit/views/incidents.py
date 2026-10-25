"""Incident timeline and explicit investigation notes."""

import pandas as pd
import plotly.express as px
import streamlit as st

from dashboards.streamlit import data
from dashboards.streamlit.ui import chart, details
from src.observability.incidents import IncidentManager


def render() -> None:
    st.title("Incident console")
    st.caption("Alert episodes · resolved episodes remain terminal")
    status = st.selectbox("Incident status", ["ALL", "OPEN", "INVESTIGATING", "RESOLVED"])
    try:
        rows = data.incidents(status)
        if not rows:
            st.info("No matching incidents.")
            return
        frame = pd.DataFrame(rows)
        a, b = st.columns(2)
        a.metric("Matching episodes", len(rows))
        b.metric("Still active", sum(r["status"] != "RESOLVED" for r in rows))
        counts = frame.groupby(["incident_type", "status"]).size().reset_index(name="episodes")
        summary, timeline, records = st.tabs(["Summary", "Timeline", "Records"])
        with summary:
            chart(
                px.bar(
                    counts,
                    x="incident_type",
                    y="episodes",
                    color="status",
                    title="Episodes by condition and status",
                    barmode="group",
                )
            )
        with timeline:
            if "detected_at" in frame and "severity" in frame:
                chart(
                    px.scatter(
                        frame,
                        x="detected_at",
                        y="incident_type",
                        color="severity",
                        hover_data=["status"],
                        title="Incident timeline",
                    )
                )
        with records:
            details(rows)
        selected = st.selectbox("Incident ID", [row["incident_id"] for row in rows])
        row = next(row for row in rows if row["incident_id"] == selected)
        st.json(row, expanded=False)
        with st.form("investigation_action"):
            note = st.text_area("Investigation note", max_chars=2000)
            submitted = st.form_submit_button(
                "Mark investigating", disabled=row["status"] != "OPEN"
            )
        if submitted:
            if not note.strip():
                st.warning("Enter an investigation note.")
            elif IncidentManager().investigate(selected, note.strip()):
                st.success("Incident marked INVESTIGATING. Refresh to read current state.")
            else:
                st.warning("Incident changed concurrently. Refresh before acting again.")
        st.caption(
            "Alertmanager resolves episodes after the condition clears. "
            "This view does not force-resolve alerts."
        )
    except Exception:
        st.error("Incident operation unavailable. Check PostgreSQL and refresh.")


if __name__ == "__main__":
    render()
