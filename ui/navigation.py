from __future__ import annotations
import streamlit as st

def navbar(available_summary: bool) -> tuple[str, str]:
    """
    Navbar en la barra lateral.
    Devuelve (mode, section) y persiste en session_state:
    - mode: 'Simplificado' | 'Técnico'
    - section: 'Operación' | 'Resumen 4.5'
    """
    mode = st.sidebar.radio("Modo", ["Simplificado", "Técnico"], index=1, key="nav_mode")
    items = ["Operación"]
    if available_summary:
        items.append("Resumen 4.5")
    section = st.sidebar.radio("Sección", items, index=0, key="nav_section")
    return mode, section
