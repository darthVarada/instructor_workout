import streamlit as st

def require_login():
    """Bloqueia o acesso caso o usuário não esteja autenticado."""
    if "user_id" not in st.session_state or st.session_state.user_id is None:
        st.error("Você precisa fazer login primeiro.")
        st.stop()

def get_user_id():
    """Retorna o ID do usuário logado."""
    return st.session_state.get("user_id", None)
