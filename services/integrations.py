# services/integrations.py
from __future__ import annotations
import urllib.parse, os
import streamlit as st
import requests

def _api_base() -> str | None:
    try:
        url = st.secrets.get("API_BASE", None)  # type: ignore[attr-defined]
    except Exception:
        url = os.getenv("API_BASE")
    if isinstance(url, str) and url.strip().startswith(("http://","https://")):
        return url.strip()
    return None

def slack_status(org_id: str) -> dict:
    base = _api_base()
    if not base:
        return {"connected": False, "error": "API_BASE no configurado"}
    try:
        r = requests.get(f"{base}/slack/status", params={"org_id": org_id}, timeout=10)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        return {"connected": False, "error": str(e)}

def slack_connect_button(org_id: str, streamlit_url: str | None = None):
    base = _api_base()
    if not base:
        st.warning("API_BASE no configurado; no se puede iniciar OAuth.")
        return
    return_url = streamlit_url or st.experimental_get_query_params().get("return", [""])[0] or st.runtime.scriptrunner.script_run_context.get_script_run_ctx().session_data.user_info.browser.server_address if hasattr(st, "runtime") else ""
    # Construimos link
    params = {"org_id": org_id, "return_url": streamlit_url or ""}
    url = f"{base}/slack/install?{urllib.parse.urlencode(params)}"
    st.link_button("Conectar Slack", url, type="primary", use_container_width=True)
