import streamlit as st

import chat
import formulario
import dashboard
from login_service import authenticate, register_user, get_user_profile


st.set_page_config(
    page_title="Instructor Workout – Personal Trainer IA",
    page_icon="🏋️",
    layout="centered",
)


# =========================
# ESTADO GLOBAL
# =========================
if "logged_user" not in st.session_state:
    st.session_state.logged_user = None

if "user_profile" not in st.session_state:
    st.session_state.user_profile = None

if "auth_mode" not in st.session_state:
    st.session_state.auth_mode = "login"  # ou "register"

if "current_page" not in st.session_state:
    st.session_state.current_page = "formulario"


# =========================
# HELPERS
# =========================
def do_logout():
    for key in [
        "logged_user",
        "user_profile",
        "auth_mode",
        "current_page",
        "chat_history",
    ]:
        if key in st.session_state:
            del st.session_state[key]
    st.session_state.auth_mode = "login"
    st.rerun()


def load_profile_if_needed():
    if not st.session_state.logged_user:
        return

    if st.session_state.user_profile is None:
        profile = get_user_profile(st.session_state.logged_user["user_id"])
        st.session_state.user_profile = profile or st.session_state.logged_user


# =========================
# TELAS PÚBLICAS (LOGIN / CADASTRO)
# =========================
def login_screen():
    st.markdown("## 👋 Bem-vindo ao Instructor Workout")

    col1, col2, col3 = st.columns([1, 2, 1])
    with col2:
        st.markdown("### Login")

        email = st.text_input("E-mail", key="login_email")
        password = st.text_input("Senha", type="password", key="login_password")

        if st.button("Entrar", use_container_width=True):
            ok, result = authenticate(email, password)
            if not ok:
                st.error(result)
            else:
                # Guarda usuário na sessão
                st.session_state.logged_user = result
                st.session_state.current_page = "formulario"
                # Carrega perfil
                load_profile_if_needed()
                st.success("Login realizado com sucesso!")
                st.rerun()

        st.markdown("---")
        if st.button("Ainda não tenho conta", use_container_width=True):
            st.session_state.auth_mode = "register"
            st.rerun()


def register_screen():
    st.markdown("## 👋 Bem-vindo ao Instructor Workout")

    col1, col2, col3 = st.columns([1, 2, 1])
    with col2:
        st.markdown("### Criar conta")

        name = st.text_input("Nome completo", key="register_name")
        email = st.text_input("E-mail", key="register_email")
        password = st.text_input("Senha", type="password", key="register_password")
        password2 = st.text_input(
            "Confirme a senha", type="password", key="register_password2"
        )

        if st.button("Criar conta", use_container_width=True):
            if password != password2:
                st.error("As senhas não conferem.")
            else:
                ok, result = register_user(name, email, password)
                if not ok:
                    st.error(result)
                else:
                    # Usuário criado → já loga e manda pro formulário
                    st.session_state.logged_user = result
                    st.session_state.user_profile = result
                    st.session_state.current_page = "formulario"
                    st.success("Conta criada com sucesso! Vamos completar seu perfil.")
                    st.rerun()

        st.markdown("---")
        if st.button("Já tenho conta", use_container_width=True):
            st.session_state.auth_mode = "login"
            st.rerun()


# =========================
# APLICAÇÃO
# =========================
user = st.session_state.logged_user

if not user:
    # Sem sidebar antes do login
    if st.session_state.auth_mode == "login":
        login_screen()
    else:
        register_screen()
else:
    # Com usuário logado → sidebar com navegação
    load_profile_if_needed()
    profile = st.session_state.user_profile

    with st.sidebar:
        st.markdown(f"### 👤 {profile.get('nome') or 'Usuário'}")
        st.markdown(f"**E-mail:** {profile.get('email')}")
        st.markdown("---")

        page = st.radio(
            "Navegação",
            ["Formulário", "Chat", "Dashboard"],
            key="sidebar_nav",
        )

        st.markdown("---")
        if st.button("Sair"):
            do_logout()

    # Renderiza página selecionada
    if page == "Formulário":
        st.session_state.current_page = "formulario"
        formulario.render(profile)
    elif page == "Chat":
        st.session_state.current_page = "chat"
        chat.render(profile)
    elif page == "Dashboard":
        st.session_state.current_page = "dashboard"
        dashboard.render(profile)
