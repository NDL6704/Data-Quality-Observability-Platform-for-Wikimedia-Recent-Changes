"""Local operator console: run with python -m streamlit run dashboards/streamlit/app.py."""

import streamlit as st

from dashboards.streamlit.views import (
    event_detail,
    incidents,
    overview,
    quality_explorer,
    replay_console,
)

st.set_page_config(
    page_title="Wikimedia · Data Operations",
    page_icon="◉",
    layout="wide",
    initial_sidebar_state="collapsed",
)
st.markdown(
    """<style>
.block-container {max-width:1400px;padding:1.4rem 1.3rem 1.5rem}
[data-testid="stMetric"] {background:#fff;border:1px solid #e2e8f0;
border-radius:10px;padding:10px 12px}
h1 {letter-spacing:-.025em;font-size:1.7rem!important;padding-bottom:.25rem!important}
h3 {font-size:1.1rem!important}
[data-testid="stSidebar"] {border-right:1px solid #e2e8f0}
[data-testid="stVerticalBlock"] {gap:.65rem}
@media(max-width:900px) {
 [data-testid="stMetricValue"] {font-size:1.35rem}
 .block-container {padding-left:1rem;padding-right:1rem}
}
</style>""",
    unsafe_allow_html=True,
)
st.sidebar.title("Wikimedia DQ")
st.sidebar.caption("Data operations · local workspace")
page = st.navigation(
    [
        st.Page(overview.render, title="Reliability & lag", url_path="overview", default=True),
        st.Page(quality_explorer.render, title="Quality explorer", url_path="quality"),
        st.Page(event_detail.render, title="Event inspector", url_path="event"),
        st.Page(replay_console.render, title="Quarantine & replay", url_path="replay"),
        st.Page(incidents.render, title="Incident console", url_path="incidents"),
    ]
)
st.sidebar.divider()
st.sidebar.button("Refresh data", key="refresh_all")
st.sidebar.caption("Refresh to read current state. Counters represent processing attempts.")
page.run()
