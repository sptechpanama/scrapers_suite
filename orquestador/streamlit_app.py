import json
from pathlib import Path

import streamlit as st

STATE_PATH = Path(__file__).parent / "state.json"

st.set_page_config(page_title="Estado del Orquestador", layout="wide")
st.title("Estado actual de los jobs")

if not STATE_PATH.exists():
    st.error(f"No se encontró el archivo {STATE_PATH.name}")
else:
    try:
        state_data = json.loads(STATE_PATH.read_text(encoding="utf-8"))
        st.json(state_data)
    except json.JSONDecodeError as exc:
        st.error(f"Error al leer el estado: {exc}")
