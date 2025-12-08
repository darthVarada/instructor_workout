import streamlit as st
from typing import Dict, Any

from groq_service import generate_training_plan


def render(user: Dict[str, Any]):
    st.title("💬 Personal Trainer IA")

    if not user:
        st.error("Você precisa fazer login primeiro.")
        return

    if "chat_history" not in st.session_state:
        st.session_state.chat_history = []

    # Renderiza histórico
    for msg in st.session_state.chat_history:
        with st.chat_message("user" if msg["role"] == "user" else "assistant"):
            st.markdown(msg["content"])

    prompt = st.chat_input("Digite sua dúvida sobre treino, recuperação, etc...")

    if prompt:
        # Adiciona pergunta ao histórico
        st.session_state.chat_history.append({"role": "user", "content": prompt})

        with st.chat_message("assistant"):
            answer = generate_training_plan(user, prompt)
            st.markdown(answer)

        # Adiciona resposta ao histórico
        st.session_state.chat_history.append({"role": "assistant", "content": answer})

        # Força atualização da tela para manter o fluxo de chat
        st.rerun()
