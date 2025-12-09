from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

# IMPORT CORRETO — respeitando sua estrutura de pastas
from instructor_workout.streamlit_app.login_service import (
    get_user_profile_by_id,
)

app = FastAPI(
    title="Instructor Workout API",
    version="1.0.0",
    description="API para consulta dos perfis dos usuários"
)

# -------------------------------------------------------------------
# 🔓 CORS LIBERADO PARA O STREAMLIT
# -------------------------------------------------------------------
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],   # pode restringir depois
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# -------------------------------------------------------------------
# 🚀 ENDPOINT DE SAÚDE /health
# -------------------------------------------------------------------
@app.get("/health")
def health():
    return {"status": "ok", "service": "profile-api"}

# -------------------------------------------------------------------
# 📌 ENDPOINT: retorna o último perfil salvo (BRONZE → GOLD fallback)
# -------------------------------------------------------------------
@app.get("/profile/{user_id}")
def get_profile(user_id: str):

    profile = get_user_profile_by_id(user_id)

    if not profile:
        return {
            "success": False,
            "message": f"Nenhum perfil encontrado para user_id={user_id}"
        }

    return {
        "success": True,
        "profile": profile
    }

# -------------------------------------------------------------------
# 📌 ENDPOINT: retorna APENAS o BRONZE (ultima versão)
# -------------------------------------------------------------------
@app.get("/latest/{user_id}")
def get_latest_profile(user_id: str):
    from instructor_workout.streamlit_app.login_service import get_latest_profile_from_bronze

    latest = get_latest_profile_from_bronze(user_id)

    if not latest:
        return {
            "success": False,
            "message": "Nenhuma versão encontrada na Bronze."
        }

    return {"success": True, "profile": latest}
