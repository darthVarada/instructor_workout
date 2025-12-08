import streamlit as st
from datetime import datetime, date
from typing import Dict, Any

from login_service import save_user_profile


def render(profile: Dict[str, Any]):
    st.title("📋 Formulário do Usuário")

    if not profile:
        st.error("Erro: nenhum perfil carregado.")
        return

    def safe_get(key: str, default=None):
        value = profile.get(key, default)
        return default if value is None else value

    # Data de nascimento com fallback
    if safe_get("data_nascimento"):
        try:
            data_nasc = datetime.strptime(str(safe_get("data_nascimento")), "%Y-%m-%d").date()
        except Exception:
            data_nasc = date.today()
    else:
        data_nasc = date.today()

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
