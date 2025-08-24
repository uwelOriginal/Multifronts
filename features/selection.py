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
    Una sola tabla (st.data_editor) con columna checkbox integrada, scroll (~10 filas) y
    botones seleccionar/deseleccionar todo. Devuelve selected_ids (lista de row_ids).
    """
    if df is None or df.empty:
        st.info("Sin datos.")
        return []

    df = _ensure_row_ids(df, id_cols)
    sel_key = f"{key}_selected_ids"
    if sel_key not in st.session_state:
        st.session_state[sel_key] = set()

    show = df[display_cols].copy()
    if rename_func is not None:
        show = rename_func(show)

    show.insert(0, approve_label, show.index.map(lambda i: df.loc[i, "__row_id__"] in st.session_state[sel_key]))

    c1, c2, c3 = st.columns([1, 1, 3])
    if c1.button("Seleccionar todo", key=f"{key}_all"):
        st.session_state[sel_key] = set(df["__row_id__"])
        show[approve_label] = True
    if c2.button("Deseleccionar todo", key=f"{key}_none"):
        st.session_state[sel_key] = set()
        show[approve_label] = False
    c3.caption(f"{approve_label}: **{len(st.session_state[sel_key])} de {len(df)}** seleccionados")

    disabled_cols = [c for c in show.columns if c != approve_label]
    edited = st.data_editor(
        show,
        key=f"{key}_editor",
        num_rows="fixed",
        use_container_width=True,
        hide_index=True,
        disabled=disabled_cols,
        height=height_px,
        column_config={
            approve_label: st.column_config.CheckboxColumn(
                approve_label, help="Marca para aprobar esta fila", default=False
            ),
        },
    )

    current_selected_idx = edited.index[edited[approve_label]].tolist()
    selected_ids = df.loc[current_selected_idx, "__row_id__"].tolist()
    st.session_state[sel_key] = set(selected_ids)
    return selected_ids
