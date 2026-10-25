"""Explicit single-row replay with a read-only current-policy preview."""

from dataclasses import asdict

import pandas as pd
import plotly.express as px
import streamlit as st

from dashboards.streamlit import data
from dashboards.streamlit.ui import chart, details
from src.recovery.replayer import replay_quarantine


def render() -> None:
    st.title("Quarantine & replay")
    st.caption("Preview current policy, then replay one original input into PostgreSQL")
    try:
        summary = data.quarantine_summary()
        if summary:
            chart(
                px.pie(
                    pd.DataFrame(summary),
                    names="replay_status",
                    values="events",
                    hole=0.65,
                    title="Quarantine disposition · all records",
                )
            )
        else:
            st.success("No quarantined records. No recovery action is currently needed.")
    except Exception:
        st.warning("Quarantine totals unavailable.")
    status = st.selectbox("Replay status", ["PENDING", "REPLAYED", "DISCARDED", "ALL"])
    code = st.text_input("Failure code (exact, optional)", max_chars=100)
    key = st.text_input("Event key (exact, optional)", max_chars=300)
    try:
        rows = data.quarantine(status, code, key)
        with st.expander("Matching records"):
            details(rows)
        if not rows:
            st.info("No matching quarantined records.")
            return
        row_id = st.selectbox("Quarantine row ID", [row["id"] for row in rows])
        row = data.quarantine_detail(row_id)
        if row is None:
            st.warning("Record no longer available.")
            return
        with st.expander("Original envelope and failure details"):
            st.json(
                {"raw_payload": row["raw_payload"], "failure_details": row["failure_details"]},
                expanded=False,
            )
        try:
            preview = data.preview_replay(row)
        except (ValueError, TypeError) as exc:
            st.warning(str(exc))
            return
        st.subheader("Current-policy preview")
        results = preview.get("current_results", [])
        if results:
            chart(
                px.bar(
                    pd.DataFrame(
                        [
                            {
                                "rule": r["rule_id"],
                                "result": "Pass" if r["passed"] else "Fail",
                                "checks": 1,
                            }
                            for r in results
                        ]
                    ),
                    x="rule",
                    y="checks",
                    color="result",
                    title="Replay preflight checks",
                )
            )
        st.json(preview, expanded=False)
        st.caption(
            "Preview writes nothing. Replay re-evaluates under current defaults and locks the row."
        )
        st.info(
            "Replay preserves original input/receipt, writes DB only and does not republish Kafka."
        )
        with st.form("replay_action"):
            confirm = st.checkbox(
                f"Replay row {row_id} using current policy", key=f"confirm_{row_id}"
            )
            submitted = st.form_submit_button(
                "Replay selected record", disabled=row["replay_status"] != "PENDING"
            )
        if submitted:
            if not confirm:
                st.warning("Select the row confirmation before replay.")
            else:
                outcome = replay_quarantine(row_id)
                st.json(asdict(outcome))
                if outcome.status == "REPLAYED":
                    st.success("Replay completed. Refresh the list to view the updated status.")
                else:
                    st.warning(f"No promotion: {outcome.status}. The original record was retained.")
    except Exception:
        st.error(
            "Quarantine operation failed. No success is assumed; refresh and inspect the record."
        )


if __name__ == "__main__":
    render()
