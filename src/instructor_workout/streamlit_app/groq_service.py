from datetime import datetime, date
from typing import Dict, Any, List

import streamlit as st
from groq import Groq


MODEL = "llama-3.1-8b-instant"
client = Groq(api_key=st.secrets["GROQ_API_KEY"])


def calcular_idade(data_nascimento: str | None) -> int | None:
    if not data_nascimento:
        return None
    try:
        # Tenta YYYY-MM-DD
        dt = datetime.strptime(data_nascimento, "%Y-%m-%d").date()
    except ValueError:
        try:
            # Tenta DD/MM/YYYY
            dt = datetime.strptime(data_nascimento, "%d/%m/%Y").date()
        except ValueError:
            return None

    hoje = date.today()
    return hoje.year - dt.year - ((hoje.month, hoje.day) < (dt.month, dt.day))


def format_user_profile(user: Dict[str, Any]) -> str:
    idade = calcular_idade(user.get("data_nascimento"))

    return f"""
Você é um personal trainer profissional. Use SEMPRE os dados abaixo para personalizar as respostas.

Perfil do aluno:
- Nome: {user.get("nome") or "Não informado"}
- Idade: {idade if idade is not None else "Não informada"}
- Data de nascimento: {user.get("data_nascimento") or "Não informada"}
- Sexo: {user.get("sexo") or "Não informado"}
- Peso (kg): {user.get("peso") or "Não informado"}
- Altura (cm): {user.get("altura") or "Não informada"}
- % Gordura: {user.get("percentual_gordura") or "Não informada"}
- Objetivo: {user.get("objetivo") or "Não informado"}
- Nível de treinamento: {user.get("nivel_treinamento") or "Não informado"}
- Restrições físicas: {user.get("restricoes_fisicas") or "Nenhuma informada"}
- Frequência semanal: {user.get("frequencia_semanal") or "Não informada"}x por semana
- Horas de sono: {user.get("horas_sono") or "Não informado"}
- Score nutricional: {user.get("nutricional_score") or "Não informado"}

Regras obrigatórias:
- Nunca ignore as restrições físicas.
- Nunca proponha mais dias de treino do que a frequência semanal informada.
- Se o usuário corrigir alguma informação, ajuste suas recomendações.
- Responda SEMPRE em português do Brasil.
"""


def generate_training_plan(user: Dict[str, Any], pergunta: str) -> str:
    """
    Gera resposta da IA usando o perfil do usuário como contexto.
    """
    system_prompt = format_user_profile(user)

    messages: List[Dict[str, str]] = [
        {"role": "system", "content": system_prompt}
    ]

    # Histórico de chat desta sessão
    history = st.session_state.get("chat_history", [])
    messages.extend(history)

    messages.append({"role": "user", "content": pergunta})

    completion = client.chat.completions.create(
        model=MODEL,
        messages=messages,
        temperature=0.6,
    )

    answer = completion.choices[0].message.content.strip()
    return answer
