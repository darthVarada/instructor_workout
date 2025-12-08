import streamlit as st

def render_sidebar():
    st.sidebar.title("Menu")

    if st.sidebar.button("💬 Chat"):
        st.session_state.page = "chat"
        st.rerun()

    if st.sidebar.button("📝 Formulário"):
        st.session_state.page = "formulario"
        st.rerun()

    if st.sidebar.button("📊 Dashboard"):
        st.session_state.page = "dashboard"
        st.rerun()

    if st.sidebar.button("Sair"):
        st.session_state.user_id = None
        st.session_state.page = "login"
        st.rerun()
