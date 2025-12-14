import streamlit as st
from datetime import datetime
from typing import Dict, Any, Optional

from login_service import (
    save_user_profile,
    get_user_profile_by_id,
    get_or_create_user_profile_by_email,  # mantido para compatibilidade futura
)

# -------------------------------------------------------------------
# HELPERS
# -------------------------------------------------------------------
def _parse_date(raw) -> Optional[datetime.date]:
    """Converte string YYYY-MM-DD ou date em date; senão retorna None."""
    if raw is None or raw == "":
        return None
    if isinstance(raw, datetime):
        return raw.date()
    if hasattr(raw, "year") and hasattr(raw, "month"):
        return raw
    try:
        return datetime.strptime(str(raw), "%Y-%m-%d").date()
    except Exception:
        return None


def _calc_bmi(peso: Optional[float], altura: Optional[float]) -> Optional[float]:
    """
    Calcula IMC. Se altura aparentar estar em centímetros (> 3), converte para metros.
    """
    try:
        if not peso or not altura:
            return None
        altura_m = altura
        if altura_m > 3:  # provavelmente o usuário colocou em cm
            altura_m = altura_m / 100.0
        if altura_m <= 0:
            return None
        return round(peso / (altura_m**2), 1)
    except Exception:
        return None


def _bmi_class(bmi: Optional[float]) -> str:
    if bmi is None:
        return "—"
    if bmi < 18.5:
        return "Abaixo do peso"
    if bmi < 25:
        return "Peso saudável"
    if bmi < 30:
        return "Sobrepeso"
    if bmi < 35:
        return "Obesidade grau I"
    if bmi < 40:
        return "Obesidade grau II"
    return "Obesidade grau III"


def _missing_fields(profile: Dict[str, Any]) -> Dict[str, str]:
    """
    Retorna campos importantes que ainda estão vazios,
    no formato {campo: label bonita}.
    """
    checks = {
        "peso": "Peso (kg)",
        "altura": "Altura (cm)",
        "gordura": "Gordura (%)",
        "objetivo": "Objetivo",
        "experiencia": "Nível de experiência",
        "dias_semana": "Dias de treino por semana",
        "minutos_por_treino": "Minutos por treino",
        "tipo_treino_principal": "Tipo de treino principal",
    }

    missing = {}
    for key, label in checks.items():
        value = profile.get(key)
        if value in (None, "", 0):
            missing[key] = label
    return missing


# -------------------------------------------------------------------
# FUNÇÃO PRINCIPAL
# -------------------------------------------------------------------
def render(profile: Dict[str, Any]) -> None:
    st.markdown("## 📝 Formulário do Usuário")

    # ----------------------------------------------------------------
    # TOPO – INFO DO USUÁRIO LOGADO + USER ID ATUAL
    # ----------------------------------------------------------------
    col_left, col_right = st.columns([2, 1])

    with col_left:
        logged_user = st.session_state.get("logged_user", {})
        nome_logado = logged_user.get("name") or logged_user.get("nickname") or "Usuário"
        email_logado = logged_user.get("email", "—")

        st.markdown(
            f"**Logado como:** `{nome_logado}`  \n"
            f"📧 **E-mail:** `{email_logado}`"
        )

    with col_right:
        current_user_id = st.session_state.get("current_user_id") or profile.get("user_id")
        if current_user_id:
            st.markdown("User ID ativo:")
            st.code(current_user_id, language="text")

    st.markdown("---")

    # ----------------------------------------------------------------
    # BLOCO – CARREGAR DADOS DO GOLD PELO USER_ID
    # ----------------------------------------------------------------
    st.markdown("### 🔍 Carregar dados do usuário (GOLD)")

    manual_user_id = st.text_input(
        "User ID (para buscar no GOLD)",
        value="",
        placeholder="Cole aqui o User ID do GOLD...",
        key="manual_user_id_input",
    )

    if st.button("📥 Carregar dados do GOLD", use_container_width=True):
        manual_user_id = manual_user_id.strip()
        if not manual_user_id:
            st.warning("Informe um User ID para buscar no GOLD.")
            return

        with st.spinner("Buscando dados no GOLD..."):
            loaded = get_user_profile_by_id(manual_user_id)

        if not loaded:
            st.error("Nenhum usuário encontrado no GOLD com esse ID.")
            return

        # O perfil carregado passa a pertencer ao usuário logado
        logged_email = logged_user.get("email")
        if logged_email:
            loaded["email"] = logged_email

        # Atualiza o user_id ativo na sessão
        st.session_state.current_user_id = loaded.get("user_id")

        # Salva como perfil oficial (sua storage atual)
        save_user_profile(loaded)

        # Atualiza perfil em memória (isso aqui é o MAIS importante pro Groq)
        st.session_state.user_profile = dict(loaded)
        profile.clear()
        profile.update(loaded)

        st.success(
            "✅ Dados carregados do GOLD e aplicados ao seu perfil oficial.\n\n"
            "Na próxima vez que você fizer login, esse será o seu perfil padrão."
        )
        st.rerun()

    st.markdown("---")

    # ----------------------------------------------------------------
    # LAYOUT GERAL: FORMULÁRIO + LADO DIREITO COM INSIGHTS
    # ----------------------------------------------------------------
    main_col, side_col = st.columns([2.3, 1])

    # =================== COLUNA PRINCIPAL ============================
    with main_col:
        st.markdown("### 👤 Informações pessoais")

        nome = st.text_input("Nome completo", value=profile.get("nome", ""))

        data_nasc = _parse_date(profile.get("data_nascimento"))
        data_nascimento = st.date_input(
            "Data de nascimento",
            value=data_nasc or datetime(2000, 1, 1).date(),
        )

        sexo_options = ["Masculino", "Feminino"]
        sexo_value = profile.get("sexo", "Masculino")
        try:
            idx_sexo = sexo_options.index(sexo_value)
        except ValueError:
            idx_sexo = 0
        sexo = st.selectbox("Sexo", sexo_options, index=idx_sexo)

        st.markdown("### ⚖️ Composição corporal")
        c1, c2, c3 = st.columns(3)
        with c1:
            peso = st.number_input(
                "Peso (kg)",
                min_value=0.0,
                step=0.1,
                value=float(profile.get("peso", 0) or 0),
            )
        with c2:
            altura = st.number_input(
                "Altura (cm ou m)",
                min_value=0.0,
                step=0.01,
                value=float(profile.get("altura", 0) or 0),
                help="Se digitar 1.80 tratamos como metros. Se digitar 180 tratamos como centímetros.",
            )
        with c3:
            gordura = st.number_input(
                "Gordura corporal (%)",
                min_value=0.0,
                step=0.1,
                value=float(profile.get("gordura", 0) or 0),
            )

        st.markdown("### 🎯 Objetivo & experiência")
        objetivo = st.text_area(
            "Objetivo principal",
            value=profile.get(
                "objetivo",
                "Ex.: Ganho de massa magra com foco em membros superiores.",
            ),
            height=80,
        )

        exp_options = ["Iniciante", "Intermediário", "Avançado"]
        experiencia_value = profile.get("experiencia", "Iniciante")
        try:
            idx_exp = exp_options.index(experiencia_value)
        except ValueError:
            idx_exp = 0
        experiencia = st.selectbox("Nível de experiência", exp_options, index=idx_exp)

        foco_options = ["Equilíbrio geral", "Estético", "Performance", "Saúde/saúde metabólica"]
        foco_value = profile.get("foco_treino", "Equilíbrio geral")
        try:
            idx_foco = foco_options.index(foco_value)
        except ValueError:
            idx_foco = 0
        foco_treino = st.selectbox(
            "Foco do programa",
            foco_options,
            index=idx_foco,
            help="Isso nos ajuda a ajustar volume, intensidade e escolha de exercícios.",
        )

        st.markdown("### 🕒 Rotina & estilo de vida")
        c1, c2, c3 = st.columns(3)
        with c1:
            dias_semana = st.number_input(
                "Dias de treino por semana",
                min_value=0,
                max_value=7,
                step=1,
                value=int(profile.get("dias_semana", 3) or 3),
            )
        with c2:
            minutos_por_treino = st.number_input(
                "Minutos por treino",
                min_value=0,
                max_value=240,
                step=5,
                value=int(profile.get("minutos_por_treino", 60) or 60),
            )
        with c3:
            passos_dia = st.number_input(
                "Passos por dia (média)",
                min_value=0,
                step=500,
                value=int(profile.get("passos_dia", 0) or 0),
            )

        c4, c5 = st.columns(2)
        with c4:
            sono_horas = st.number_input(
                "Horas de sono por noite (média)",
                min_value=0.0,
                max_value=24.0,
                step=0.5,
                value=float(profile.get("sono_horas", 7) or 7),
            )
        with c5:
            estresse_nivel = st.slider(
                "Nível de estresse atual",
                min_value=1,
                max_value=5,
                value=int(profile.get("estresse_nivel", 3) or 3),
                help="1 = muito baixo, 5 = muito alto",
            )

        st.markdown("### 🏋️‍♂️ Preferências de treino & restrições")
        tipo_options = [
            "Musculação tradicional",
            "Full body",
            "Upper/Lower",
            "Push/Pull/Legs",
            "Treino em casa",
            "Funcional / HIIT",
        ]
        tipo_value = profile.get("tipo_treino_principal", "Musculação tradicional")
        try:
            idx_tipo = tipo_options.index(tipo_value)
        except ValueError:
            idx_tipo = 0
        tipo_treino_principal = st.selectbox("Tipo de treino preferido", tipo_options, index=idx_tipo)

        equipamentos_options = [
            "Academia completa",
            "Academia simples / condomínio",
            "Apenas halteres / elásticos em casa",
            "Sem equipamentos",
        ]
        equip_value = profile.get("acesso_equipamentos", "Academia completa")
        try:
            idx_equip = equipamentos_options.index(equip_value)
        except ValueError:
            idx_equip = 0
        acesso_equipamentos = st.selectbox("Acesso a equipamentos", equipamentos_options, index=idx_equip)

        dores_articulacoes = st.text_area(
            "Dores / limitações articulares",
            value=profile.get(
                "dores_articulacoes",
                "Ex.: desconforto em ombro direito, evitar carga muito alta em agachamento.",
            ),
            height=80,
        )

        restricoes_medicas = st.text_area(
            "Restrições médicas ou diagnósticos relevantes",
            value=profile.get("restricoes_medicas", "Ex.: hipertensão controlada, hérnia de disco, etc."),
            height=80,
        )

        observacoes_gerais = st.text_area(
            "Observações gerais para o treinador",
            value=profile.get("observacoes_gerais", ""),
            height=80,
        )

        st.markdown("---")
        if st.button("💾 Salvar alterações", use_container_width=True):
            updated = {
                # Identidade
                "user_id": st.session_state.get("current_user_id", profile.get("user_id")),
                "email": logged_user.get("email"),

                # Pessoal
                "nome": nome,
                "data_nascimento": data_nascimento.strftime("%Y-%m-%d") if data_nascimento else None,
                "sexo": sexo,

                # Corpo
                "peso": float(peso) if peso else None,
                "altura": float(altura) if altura else None,
                "gordura": float(gordura) if gordura else None,

                # Objetivo / experiência
                "objetivo": objetivo,
                "experiencia": experiencia,
                "foco_treino": foco_treino,

                # Rotina
                "dias_semana": int(dias_semana) if dias_semana else None,
                "minutos_por_treino": int(minutos_por_treino) if minutos_por_treino else None,
                "passos_dia": int(passos_dia) if passos_dia else None,
                "sono_horas": float(sono_horas) if sono_horas else None,
                "estresse_nivel": int(estresse_nivel) if estresse_nivel else None,

                # Preferências / restrições
                "tipo_treino_principal": tipo_treino_principal,
                "acesso_equipamentos": acesso_equipamentos,
                "dores_articulacoes": dores_articulacoes,
                "restricoes_medicas": restricoes_medicas,
                "observacoes_gerais": observacoes_gerais,
            }

            # salva na sua storage atual
            save_user_profile(updated)

            # 🔥 isso é o que garante que o Groq “aprende” na hora:
            st.session_state.user_profile = dict(updated)
            profile.clear()
            profile.update(updated)

            st.success("✅ Perfil atualizado com sucesso!")
            st.rerun()

    # =================== COLUNA LATERAL – INSIGHTS ====================
    with side_col:
        st.markdown("### 📊 Insights rápidos")

        bmi = _calc_bmi(
            profile.get("peso") if "peso" in profile else peso,
            profile.get("altura") if "altura" in profile else altura,
        )
        bmi_txt = "—" if bmi is None else str(bmi)
        bmi_class = _bmi_class(bmi)
        st.metric("IMC estimado", bmi_txt, bmi_class)

        try:
            dias_val = profile.get("dias_semana", dias_semana)
            mins_val = profile.get("minutos_por_treino", minutos_por_treino)
        except NameError:
            dias_val = profile.get("dias_semana", 0)
            mins_val = profile.get("minutos_por_treino", 0)

        if dias_val and mins_val:
            total_semana = dias_val * mins_val
            st.metric("Minutos/semana previstos", f"{total_semana} min")
            if total_semana < 150:
                st.caption("🔸 Abaixo das recomendações mínimas de saúde (150 min/semana).")
            else:
                st.caption("✅ Dentro ou acima do mínimo recomendado (150 min/semana).")

        st.markdown("### 🧩 Dados faltando")
        current_profile = dict(profile)
        current_profile.update(
            {
                "peso": locals().get("peso", current_profile.get("peso")),
                "altura": locals().get("altura", current_profile.get("altura")),
                "gordura": locals().get("gordura", current_profile.get("gordura")),
                "objetivo": locals().get("objetivo", current_profile.get("objetivo")),
                "experiencia": locals().get("experiencia", current_profile.get("experiencia")),
                "dias_semana": locals().get("dias_semana", current_profile.get("dias_semana")),
                "minutos_por_treino": locals().get("minutos_por_treino", current_profile.get("minutos_por_treino")),
                "tipo_treino_principal": locals().get("tipo_treino_principal", current_profile.get("tipo_treino_principal")),
            }
        )

        missing = _missing_fields(current_profile)
        if not missing:
            st.success("Todos os dados essenciais para montar um bom plano já foram preenchidos. 🔥")
        else:
            st.warning("Preencha estes campos para ter um plano mais personalizado:")
            for label in missing.values():
                st.markdown(f"- {label}")
