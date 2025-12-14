import os
import json
import uuid
from datetime import datetime, date
import re
import pandas as pd
import streamlit as st
from groq import Groq
from streamlit_cookies_manager import EncryptedCookieManager

st.set_page_config(page_title="Instructor Workout – Personal Trainer IA", layout="centered")

cookies = EncryptedCookieManager(prefix="iw/", password="instructor-workout-123")

if not cookies.ready():
    st.stop()

client = Groq(api_key=os.getenv("GROQ_API_KEY"))
MODEL = "llama-3.1-8b-instant"


# =========================
# COOKIES
# =========================
def salvar_perfil(perfil):
    cookies["user_profile"] = json.dumps(perfil)
    cookies.save()

def carregar_perfil():
    if cookies.get("user_profile"):
        return json.loads(cookies.get("user_profile"))
    return None

def excluir_perfil():
    cookies["user_profile"] = ""
    cookies["recommended_workouts"] = ""
    cookies.save()
    st.session_state.clear()
    st.rerun()

def carregar_treinos():
    if cookies.get("recommended_workouts"):
        return json.loads(cookies.get("recommended_workouts"))
    return []

def salvar_treino(pergunta, resposta, user_id):
    treino = {
        "id": str(uuid.uuid4()),
        "user_id": user_id,
        "data": datetime.now().strftime("%d/%m/%Y %H:%M"),
        "pergunta": pergunta,
        "resposta": resposta
    }
    treinos = carregar_treinos()
    treinos.append(treino)
    cookies["recommended_workouts"] = json.dumps(treinos)
    cookies.save()


# =========================
# IA COM MEMÓRIA DE CONVERSA
# =========================
def calcular_idade(data_nascimento):
    try:
        # Tenta formato brasileiro primeiro: DD/MM/YYYY
        nascimento = datetime.strptime(data_nascimento, "%d/%m/%Y").date()
    except ValueError:
        # Se falhar, tenta formato internacional: YYYY-MM-DD
        nascimento = datetime.strptime(data_nascimento, "%Y-%m-%d").date()

    hoje = date.today()
    return hoje.year - nascimento.year - ((hoje.month, hoje.day) < (nascimento.month, nascimento.day))



def ask_groq(pergunta, perfil):

    system_prompt = f"""
Você é um personal trainer profissional.

REGRAS OBRIGATÓRIAS:
- Nunca ignore os dados do perfil
- Sempre respeite a frequência semanal: {perfil['frequencia_semanal']} vezes por semana
- Nunca monte treino com mais dias que essa frequência
- Respeite TODAS as restrições físicas: {perfil['restricoes_fisicas']}
- Se o usuário corrigir algo, você DEVE ajustar
- Nunca invente dias extras
- Nunca contradiga respostas anteriores sem explicação

Perfil do aluno:
Nome: {perfil['nome']}
Idade: {calcular_idade(perfil['data_nascimento'])}
Objetivo: {perfil['objetivo']}
Nível: {perfil['nivel_treinamento']}
Restrições: {perfil['restricoes_fisicas']}
Frequência semanal: {perfil['frequencia_semanal']}x
"""

    messages = [{"role": "system", "content": system_prompt}]

    # ✅ ENVIA TODO O HISTÓRICO PARA O MODELO
    if "chat" in st.session_state:
        for msg in st.session_state.chat:
            messages.append(msg)

    messages.append({"role": "user", "content": pergunta})

    completion = client.chat.completions.create(
        model=MODEL,
        messages=messages,
        temperature=0.6
    )

    return completion.choices[0].message.content.strip()


# =========================
# TELAS
# =========================
def tela_cadastro():
    st.title("🏋️ Cadastro Inicial")

    with st.form("cadastro"):
        nome = st.text_input("Nome")
        data_nascimento = st.date_input("Data de nascimento", min_value=date(1950,1,1), max_value=date(2025,12,31))
        sexo = st.selectbox("Sexo", ["Masculino", "Feminino"])
        peso = st.number_input("Peso (kg)", 1)
        altura = st.number_input("Altura (cm)", 1)
        gordura = st.number_input("Gordura (%)", 1)
        objetivo = st.selectbox("Objetivo", ["Hipertrofia", "Emagrecimento", "Condicionamento"])
        nivel = st.selectbox("Nível", ["Iniciante", "Intermediário", "Avançado"])
        restricoes = st.text_input("Restrições físicas")
        freq = st.slider("Frequência semanal", 1, 7)
        sono = st.slider("Horas de sono", 1, 12)
        score = st.slider("Score nutricional", 0, 100)

        if st.form_submit_button("Salvar"):
            perfil = {
                "user_id": str(uuid.uuid4()),
                "data_registro": str(datetime.now()),
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
                "nutricional_score": score
            }

            salvar_perfil(perfil)
            st.success("Perfil criado com sucesso!")
            st.rerun()


def tela_chat(perfil):
    st.title("💬 Personal Trainer")

    if "chat" not in st.session_state:
        st.session_state.chat = []

    if "last_question" not in st.session_state:
        st.session_state.last_question = ""
        st.session_state.last_answer = ""

    for msg in st.session_state.chat:
        st.chat_message(msg["role"]).write(msg["content"])

    pergunta = st.chat_input("Digite sua pergunta...")

    if pergunta:
        st.session_state.chat.append({"role":"user","content":pergunta})
        resposta = ask_groq(pergunta, perfil)
        st.session_state.chat.append({"role":"assistant","content":resposta})

        st.session_state.last_question = pergunta
        st.session_state.last_answer = resposta
        st.rerun()

    if st.session_state.last_answer:
        if st.button("💾 Salvar treino recomendado"):
            salvar_treino(st.session_state.last_question, st.session_state.last_answer, perfil["user_id"])
            st.success("Treino salvo com sucesso!")
            st.rerun()


def tela_treinos():
    st.title("📋 Treinos Recomendados")

    treinos = carregar_treinos()

    if not treinos:
        st.info("Nenhum treino salvo ainda.")
        return

    for treino in treinos:
        st.markdown(f"### {treino['data']}")
        st.write(treino["resposta"])


def tela_atualizacao(perfil):
    st.title(f"Olá {perfil['nome']} 👋")
    st.subheader("Atualize os dados do seu treino")

    with st.form("update"):
        perfil["peso"] = st.number_input("Peso", value=int(perfil["peso"]))
        perfil["altura"] = st.number_input("Altura", value=int(perfil["altura"]))
        perfil["percentual_gordura"] = st.number_input("Gordura (%)", value=int(perfil["percentual_gordura"]))
        perfil["objetivo"] = st.selectbox("Objetivo", ["Hipertrofia","Emagrecimento","Condicionamento"])
        perfil["nivel_treinamento"] = st.selectbox("Nível", ["Iniciante","Intermediário","Avançado"])
        perfil["restricoes_fisicas"] = st.text_input("Restrições", value=perfil["restricoes_fisicas"])
        perfil["frequencia_semanal"] = st.slider("Frequência",1,7,value=int(perfil["frequencia_semanal"]))
        perfil["horas_sono"] = st.slider("Sono",1,12,value=int(perfil["horas_sono"]))
        perfil["nutricional_score"] = st.slider("Nutrição",0,100,value=int(perfil["nutricional_score"]))

        if st.form_submit_button("Atualizar"):
            salvar_perfil(perfil)
            st.success("Perfil atualizado!")
            st.rerun()

    if st.button("❌ Excluir meu perfil"):
        excluir_perfil()


# =========================
# APP
# =========================
perfil = carregar_perfil()

menu = st.sidebar.radio("Menu", ["Chat", "Treinos Recomendados", "Atualizar Perfil"])

if not perfil:
    tela_cadastro()
else:
    if menu == "Chat":
        tela_chat(perfil)
    elif menu == "Treinos Recomendados":
        tela_treinos()
    elif menu == "Atualizar Perfil":
        tela_atualizacao(perfil)
