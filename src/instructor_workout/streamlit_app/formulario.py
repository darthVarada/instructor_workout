import streamlit as st
from datetime import datetime, date
from typing import Dict, Any

from login_service import save_user_profile, get_user_profile_by_id


def render(profile: Dict[str, Any]):
    st.title("📋 Formulário do Usuário")

    if not profile:
        st.error("Erro: nenhum perfil carregado.")
        return

    # ============================================================
    # GARANTE QUE user_id SEMPRE ESTEJA DISPONÍVEL
    # ============================================================
    if "user_id" not in profile or not profile["user_id"]:
        profile["user_id"] = st.session_state.get("user_profile", {}).get("user_id", "")

    st.markdown(f"**🆔 User ID carregado:** `{profile.get('user_id', '')}`")

    # ============================================================
    # FUNÇÃO SEGURA PARA PEGAR CAMPOS
    # ============================================================
    def safe_get(key: str, default=None):
        value = profile.get(key, default)
        return default if value is None else value

    # ============================================================
    # CAMPO PARA CARREGAR PERFIL DIRETAMENTE DO GOLD/dim_user
    # ============================================================
    st.markdown("### 🔍 Carregar dados do usuário (GOLD)")

    manual_user_id = st.text_input(
        "User ID (para buscar no GOLD)",
        value=profile.get("user_id", ""),
        key="manual_user_id_input",
    )

    if st.button("📥 Carregar dados do GOLD"):
        if not manual_user_id:
            st.error("Digite um User ID válido.")
        else:
            print(f"🔎 [DEBUG] Botão 'Carregar dados do GOLD' clicado com user_id={manual_user_id}")
            loaded = get_user_profile_by_id(manual_user_id)

            if loaded:
                st.success("Dados carregados com sucesso do GOLD!")

                # Atualiza perfil local
                st.session_state.user_profile = loaded

                # Atualiza o objeto atual para preencher campos
                profile.clear()
                profile.update(loaded)

                st.rerun()
            else:
                st.error("Nenhum registro encontrado no GOLD para esse User ID.")

    st.markdown("---")

    # ============================================================
    # TRATAMENTO DA DATA DE NASCIMENTO
    # ============================================================
    if safe_get("data_nascimento"):
        try:
            data_nasc = datetime.strptime(
                str(safe_get("data_nascimento")), "%Y-%m-%d"
            ).date()
        except Exception:
            data_nasc = date.today()
    else:
        data_nasc = date.today()

    # ============================================================
    # FORMULÁRIO PRINCIPAL
    # ============================================================
    with st.form("form_profile"):
        nome = st.text_input("Nome completo", value=safe_get("nome", ""))

        data_nascimento = st.date_input("Data de nascimento", value=data_nasc)

        sexo = st.selectbox(
            "Sexo",
            ["Masculino", "Feminino"],
            index=0 if safe_get("sexo", "Masculino") == "Masculino" else 1,
        )

        peso = st.number_input("Peso (kg)", value=float(safe_get("peso", 70)))
        altura = st.number_input("Altura (cm)", value=float(safe_get("altura", 170)))
        gordura = st.number_input(
            "Gordura (%)", value=float(safe_get("percentual_gordura", 20))
        )

        objetivo_opcoes = ["Hipertrofia", "Emagrecimento", "Condicionamento"]
        objetivo_val = safe_get("objetivo", "Hipertrofia")
        objetivo = st.selectbox(
            "Objetivo",
            objetivo_opcoes,
            index=objetivo_opcoes.index(objetivo_val)
            if objetivo_val in objetivo_opcoes
            else 0,
        )

        nivel_opcoes = ["Iniciante", "Intermediário", "Avançado"]
        nivel_val = safe_get("nivel_treinamento", "Iniciante")
        nivel = st.selectbox(
            "Nível de treinamento",
            nivel_opcoes,
            index=nivel_opcoes.index(nivel_val)
            if nivel_val in nivel_opcoes
            else 0,
        )

        restricoes = st.text_input(
            "Restrições físicas", value=safe_get("restricoes_fisicas", "")
        )

        freq = st.slider(
            "Frequência semanal (dias de treino)",
            1,
            7,
            value=int(safe_get("frequencia_semanal", 3)),
        )

        sono = st.slider(
            "Horas de sono",
            1,
            12,
            value=int(safe_get("horas_sono", 7)),
        )

        score = st.slider(
            "Score nutricional",
            0,
            100,
            value=int(safe_get("nutricional_score", 50)),
        )

        submitted = st.form_submit_button("Salvar informações")

    # ============================================================
    # SALVA NO GOLD + BRONZE AO CLICAR EM SALVAR
    # ============================================================
    if submitted:
        updated_profile = {
            **profile,
            "nome": nome,
            "data_nascimento": data_nascimento.strftime("%Y-%m-%d"),
            "sexo": sexo,
            "peso": peso,
            "altura": altura,
            "percentual_gordura": gordura,
            "objetivo": objetivo,
            "nivel_treinamento": nivel,
            "restricoes_fisicas": restricoes,
            "frequencia_semanal": freq,
            "horas_sono": sono,
            "nutricional_score": score,
        }

        save_user_profile(updated_profile)
        st.session_state.user_profile = updated_profile

        st.success("Perfil atualizado com sucesso!")
        st.rerun()
