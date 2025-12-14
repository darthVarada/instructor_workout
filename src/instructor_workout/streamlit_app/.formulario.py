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
    # NOVO: CAMPOS OBRIGATÓRIOS + REALCE DE QUEM ESTÁ FALTANDO
    # ============================================================
    REQUIRED_FIELDS = {
        "nome": "Nome completo",
        "data_nascimento": "Data de nascimento",
        "sexo": "Sexo",
        "peso": "Peso (kg)",
        "altura": "Altura (cm)",
        "percentual_gordura": "Gordura (%)",
        "objetivo": "Objetivo",
        "nivel_treinamento": "Nível de treinamento",
        "frequencia_semanal": "Frequência semanal (dias de treino)",
        "horas_sono": "Horas de sono",
        "nutricional_score": "Score nutricional",
    }

    def is_missing(key: str) -> bool:
        v = profile.get(key)
        if v is None:
            return True
        if isinstance(v, str) and v.strip() == "":
            return True
        if isinstance(v, (int, float)) and v == 0:
            return True
        return False

    missing_keys = [k for k in REQUIRED_FIELDS if is_missing(k)]

    if missing_keys:
        missing_labels = [REQUIRED_FIELDS[k] for k in missing_keys]
        st.warning(
            "⚠️ Alguns campos importantes ainda estão vazios ou incompletos: "
            + ", ".join(f"**{label}**" for label in missing_labels)
            + ". Preencha-os para que os treinos e recomendações fiquem mais personalizados."
        )

    def highlight_label(text: str, key: str) -> str:
        """
        Deixa o label vermelho e em negrito se o campo estiver faltando.
        Usa sintaxe de markdown do próprio Streamlit (:red[...]).
        """
        if key in missing_keys:
            return f":red[**{text} (obrigatório)**]"
        return text

    # ============================================================
    # CAMPO PARA CARREGAR PERFIL DIRETAMENTE DO GOLD/dim_user
    # ============================================================
    st.markdown("### 🔍 Carregar dados do usuário (GOLD)")

    manual_user_id = st.text_input(
        "User ID (para buscar no GOLD)",
        value="",  # <-- sempre vazio ao entrar
        placeholder="Cole aqui o User ID que veio do GOLD (se tiver)...",
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

                # Dados do usuário autenticado (nome, email, etc)
                base_user = (st.session_state.get("logged_user") or {}).copy()

                # Mescla:
                # - base_user: name/email da conta logada
                # - loaded: dados de treino / formulário do GOLD
                # Se o GOLD tiver algum campo, ele sobrescreve o base_user,
                # EXCETO user_id que sempre será o manual_user_id escolhido.
                merged = {
                    **base_user,
                    **loaded,
                    "user_id": manual_user_id,
                }

                # Atualiza perfil na sessão
                st.session_state.user_profile = merged

                # Atualiza o objeto atual para preencher campos do formulário
                profile.clear()
                profile.update(merged)

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
        # -----------------------------
        # Informações pessoais
        # -----------------------------
        nome = st.text_input(
            highlight_label("Nome completo", "nome"),
            value=safe_get("nome", ""),
        )

        data_nascimento = st.date_input(
            highlight_label("Data de nascimento", "data_nascimento"),
            value=data_nasc,
        )

        sexo = st.selectbox(
            highlight_label("Sexo", "sexo"),
            ["Masculino", "Feminino"],
            index=0 if safe_get("sexo", "Masculino") == "Masculino" else 1,
        )

        # -----------------------------
        # Medidas corporais
        # -----------------------------
        peso = st.number_input(
            highlight_label("Peso (kg)", "peso"),
            value=float(safe_get("peso", 70)),
        )
        altura = st.number_input(
            highlight_label("Altura (cm)", "altura"),
            value=float(safe_get("altura", 170)),
        )
        gordura = st.number_input(
            highlight_label("Gordura (%)", "percentual_gordura"),
            value=float(safe_get("percentual_gordura", 20)),
        )

        # -----------------------------
        # Treino
        # -----------------------------
        objetivo_opcoes = ["Hipertrofia", "Emagrecimento", "Condicionamento"]
        objetivo_val = safe_get("objetivo", "Hipertrofia")
        objetivo = st.selectbox(
            highlight_label("Objetivo", "objetivo"),
            objetivo_opcoes,
            index=objetivo_opcoes.index(objetivo_val)
            if objetivo_val in objetivo_opcoes
            else 0,
        )

        nivel_opcoes = ["Iniciante", "Intermediário", "Avançado"]
        nivel_val = safe_get("nivel_treinamento", "Iniciante")
        nivel = st.selectbox(
            highlight_label("Nível de treinamento", "nivel_treinamento"),
            nivel_opcoes,
            index=nivel_opcoes.index(nivel_val)
            if nivel_val in nivel_opcoes
            else 0,
        )

        restricoes = st.text_input(
            "Restrições físicas", value=safe_get("restricoes_fisicas", "")
        )

        freq = st.slider(
            highlight_label("Frequência semanal (dias de treino)", "frequencia_semanal"),
            1,
            7,
            value=int(safe_get("frequencia_semanal", 3)),
        )

        # -----------------------------
        # Sono + Nutrição
        # -----------------------------
        sono = st.slider(
            highlight_label("Horas de sono", "horas_sono"),
            1,
            12,
            value=int(safe_get("horas_sono", 7)),
        )

        score = st.slider(
            highlight_label("Score nutricional", "nutricional_score"),
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
