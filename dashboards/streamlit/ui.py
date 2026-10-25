"""Shared visual language and safe static detail tables, without Glide grids."""

import json
from datetime import datetime
from typing import Any

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

COLORS = ["#0f766e", "#2563eb", "#d97706", "#dc2626", "#7c3aed"]
px.defaults.color_discrete_sequence = COLORS


def chart(fig: go.Figure) -> None:
    """Consistent chart styling; retain zoom/hover/export interactions."""
    fig.update_layout(
        template="plotly_white",
        colorway=COLORS,
        height=255,
        margin={"l": 12, "r": 12, "t": 42, "b": 30},
        title={"font": {"size": 15}, "x": 0},
        font={"family": "Segoe UI, sans-serif", "size": 11},
        legend={"orientation": "h", "y": -0.25, "x": 0, "title_text": ""},
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
    )
    fig.update_xaxes(title_text="", automargin=True)
    fig.update_yaxes(title_text="", automargin=True, rangemode="tozero")
    st.plotly_chart(
        fig,
        width="stretch",
        theme=None,
        config={"displaylogo": False, "displayModeBar": False, "scrollZoom": False},
    )


def details(rows: list[dict[str, Any]], limit: int = 10) -> None:
    """Flatten nested values and avoid the interactive grid's JS renderer."""
    if not rows:
        st.info("No matching records.")
        return
    flat = [
        {
            k: json.dumps(v, default=str) if isinstance(v, (dict, list)) else str(v)
            for k, v in row.items()
        }
        for row in rows[:limit]
    ]
    st.table(pd.DataFrame(flat))
    if len(rows) > limit:
        st.caption(f"Showing {limit} of {len(rows)} returned records; refine filters for more.")


def parse_instant(value: str) -> datetime | None:
    """Require an explicit offset; never silently interpret local time as UTC."""
    if not value.strip():
        return None
    result = datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
    if result.tzinfo is None or result.utcoffset() is None:
        raise ValueError("Include timezone, for example 2026-10-25T08:48:46Z")
    return result
