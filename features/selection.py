import pandas as pd
import streamlit as st

def _ensure_row_ids(df: pd.DataFrame, id_cols: list[str]) -> pd.DataFrame:
    df = df.copy()
    df["__row_id__"] = df[id_cols].astype(str).agg("|".join, axis=1)
    return df

def selection_to_dataframe(df: pd.DataFrame, selected_ids: list[str], id_cols: list[str]):
    if df is None or df.empty or not selected_ids:
        return df.iloc[0:0]
    df = _ensure_row_ids(df, id_cols)
    return df[df["__row_id__"].isin(selected_ids)].drop(columns="__row_id__")

def render_selectable_editor(
    df: pd.DataFrame,
    id_cols: list[str],
    display_cols: list[str],
    key: str,
    approve_label: str = "Aprobar",
    height_px: int = 360,
    rename_func=None,
):
    """
    Tabla con st.data_editor + columna checkbox.
    - IDs de negocio en __row_id__ (oculta) para mapear selección sin depender del índice.
    - Estado persistente en st.session_state[<key>_selected_ids] (set de row_ids).
    - Botones de selección masiva como st.form_submit_button (válidos dentro de forms).
    - Al seleccionar/deseleccionar todo se reinicia el estado del editor para reflejar el cambio inmediatamente.
    """
    if df is None or df.empty:
        st.info("Sin datos.")
        return []

    df = _ensure_row_ids(df, id_cols)
    sel_key = f"{key}_selected_ids"
    editor_key = f"{key}_editor"

    if sel_key not in st.session_state:
        st.session_state[sel_key] = set()

    # Data a mostrar: columnas visibles + __row_id__ (oculta)
    show = df[display_cols + ["__row_id__"]].copy()
    if rename_func is not None:
        show = rename_func(show)

    # Casilla inicial basada en session_state (no pasamos 'value' al widget)
    show.insert(0, approve_label, show["__row_id__"].map(lambda rid: rid in st.session_state[sel_key]))

    # Acciones masivas dentro de forms
    c1, c2, c3 = st.columns([1, 1, 3])
    select_all = c1.form_submit_button("Seleccionar todo", use_container_width=True)
    clear_all  = c2.form_submit_button("Deseleccionar todo", use_container_width=True)

    # Aplica acciones (independientes) y reinicia el estado del editor para evitar que el widget retenga checks previos
    if clear_all:
        st.session_state[sel_key] = set()
        if editor_key in st.session_state:
            del st.session_state[editor_key]
    if select_all:
        st.session_state[sel_key] = set(df["__row_id__"])
        if editor_key in st.session_state:
            del st.session_state[editor_key]

    # Recalcular booleans según el estado actual tras acciones
    show[approve_label] = show["__row_id__"].map(lambda rid: rid in st.session_state[sel_key])

    # Columnas no editables (todo menos el checkbox)
    disabled_cols = [c for c in show.columns if c != approve_label]

    # Configurar columnas (ocultar __row_id__ si es posible)
    col_cfg = {
        approve_label: st.column_config.CheckboxColumn(
            approve_label,
            help="Marca para aprobar esta fila"
        ),
    }
    try:
        col_cfg["__row_id__"] = st.column_config.TextColumn(
            "__row_id__", help="row-id", disabled=True, hidden=True
        )
    except Exception:
        pass

    edited = st.data_editor(
        show,
        key=editor_key,
        num_rows="fixed",
        use_container_width=True,
        hide_index=True,
        disabled=disabled_cols,
        height=height_px,
        column_config=col_cfg,
    )

    # Determinar seleccionados del resultado editado SIN depender del índice base
    if "__row_id__" in edited.columns:
        selected_ids = edited.loc[edited[approve_label], "__row_id__"].tolist()
    else:
        current_selected_idx = edited.index[edited[approve_label]].tolist()
        selected_ids = df.loc[current_selected_idx, "__row_id__"].tolist()

    # Persistir selección y mostrar resumen abajo
    st.session_state[sel_key] = set(selected_ids)

    return selected_ids
