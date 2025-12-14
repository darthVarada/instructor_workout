import streamlit as st

import chat
import formulario
import dashboard

from login_service import (
    authenticate,
    register_user,
    get_user_profile_by_id,
)

# Exemplo: pegue o user_id logado de onde você já estiver pegando (auth, sessão, etc.)
def get_current_user_id():
    # TODO: trocar por sua lógica real
    return st.session_state.get("current_user_id", "USER_ATUAL_EXEMPLO")

if "current_user_id" not in st.session_state:
    st.session_state.current_user_id = get_current_user_id()

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
    st.session_state.auth_mode = "login"

if "current_page" not in st.session_state:
    st.session_state.current_page = "formulario"

if "chat_history" not in st.session_state:
    st.session_state.chat_history = []

def importar_dados_de_outro_usuario_ui():
    st.subheader("Importar dados de outro usuário")

    source_user_id = st.text_input("User ID de origem (de quem você quer copiar os dados)")
    current_user_id = st.session_state.current_user_id

    if st.button("Puxar dados desse usuário"):
        if not source_user_id:
            st.error("Informe um user_id de origem.")
            return

        try:
            # 1) Carrega o pacote completo do usuário de origem
            pacote = carregar_pacote_usuario(source_user_id)

            if not pacote:
                st.warning("Não encontrei dados para esse user_id.")
                return

            # 2) Copia esses dados para o usuário atual
            copiar_pacote_para_usuario_atual(
                pacote_origem=pacote,
                target_user_id=current_user_id,
            )

            # 3) Opcional: atualizar o formulário no estado da sessão
            st.session_state["form_data"] = pacote.get("formulario", {})

            st.success(
                f"Dados do usuário {source_user_id} copiados para a sua conta ({current_user_id})."
            )
        except Exception as e:
            st.error(f"Erro ao importar dados: {e}")


# =========================
# HELPER — LOGOUT
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


# =========================
# HELPER — LOAD PROFILE (BRONZE → GOLD)
# =========================
def load_profile_if_needed():
    if not st.session_state.logged_user:
        return

    if st.session_state.user_profile is None:
        user_id = st.session_state.logged_user["user_id"]

        latest = get_user_profile_by_id(user_id)

        st.session_state.user_profile = latest or st.session_state.logged_user


# =========================
# LOGIN SCREEN
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
                st.session_state.logged_user = result
                load_profile_if_needed()
                st.session_state.current_page = "formulario"

                st.success("Login realizado com sucesso!")
                st.rerun()

        st.markdown("---")
        if st.button("Ainda não tenho conta", use_container_width=True):
            st.session_state.auth_mode = "register"
            st.rerun()


# =========================
# REGISTER SCREEN
# =========================
def register_screen():
    st.markdown("## 👋 Bem-vindo ao Instructor Workout")

    col1, col2, col3 = st.columns([1, 2, 1])

    with col2:
        st.markdown("### Criar conta")

        name = st.text_input("Nome completo", key="register_name")
        email = st.text_input("E-mail", key="register_email")
        password = st.text_input("Senha", type="password", key="register_password")
        password2 = st.text_input("Confirme a senha", type="password", key="register_password2")

        if st.button("Criar conta", use_container_width=True):
            if password != password2:
                st.error("As senhas não conferem.")
            else:
                ok, result = register_user(name, email, password)

                if not ok:
                    st.error(result)
                else:
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
    if st.session_state.auth_mode == "login":
        login_screen()
    else:
        register_screen()

else:
    load_profile_if_needed()
    profile = st.session_state.user_profile or {}
    logged = st.session_state.logged_user or {}

    # Nome: prefere 'nome' do profile, senão 'name' do usuário logado
    display_name = profile.get("nome") or logged.get("name") or "Usuário"

    # E-mail: prefere do profile, senão do usuário logado
    display_email = profile.get("email") or logged.get("email") or "-"

    # SIDEBAR
    with st.sidebar:
        st.markdown(f"### 👤 {display_name}")
        st.markdown(f"**E-mail:** {display_email}")
        st.markdown("---")

        page = st.radio("Navegação", ["Formulário", "Chat", "Dashboard"], key="sidebar_nav")


        st.markdown("---")
        if st.button("Sair"):
            do_logout()

    # PÁGINAS
    if page == "Formulário":
        formulario.render(profile)

    elif page == "Chat":
        chat.render(profile)

    elif page == "Dashboard":
        dashboard.render(profile)
