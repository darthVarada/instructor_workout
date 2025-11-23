# ingestion/api_test.py
import os
import requests
from textwrap import indent

API_BASE = "https://api.hevyapp.com"
API_KEY = (os.getenv("HEVY_API_KEY") or "").strip()

def mask_key(k: str) -> str:
    if not k:
        return "(vazia)"
    if len(k) <= 8:
        return "***"
    return f"{k[:4]}...{k[-4:]}"

def try_request(path: str, headers: dict) -> requests.Response:
    url = f"{API_BASE}{path}"
    r = requests.get(url, headers=headers, timeout=30)
    return r

def main():
    print("=== Hevy API quick auth test ===")
    print(f"HEVY_API_KEY presente? {'sim' if API_KEY else 'não'}")
    print(f"HEVY_API_KEY (mascarada): {mask_key(API_KEY)}")

    if not API_KEY:
        print("ERRO: defina a variável de ambiente HEVY_API_KEY antes de continuar.")
        return

    # Tentamos os padrões mais comuns de auth da Hevy
    header_variants = [
        {"accept": "application/json", "api-key": API_KEY},           # mais comum
        {"accept": "application/json", "x-api-key": API_KEY},         # fallback
        {"accept": "application/json", "Authorization": f"Bearer {API_KEY}"},  # raríssimo, mas testamos
    ]

    # 1) Sanity check simples: /v1/workouts/count
    print("\n-> Testando /v1/workouts/count com diferentes headers...")
    ok_any = False
    for i, h in enumerate(header_variants, 1):
        print(f"\n[Variante {i}] headers = { {k:(v if k!='api-key' and k!='x-api-key' and k!='Authorization' else '(mascarado)') for k,v in h.items()} }")
        r = try_request("/v1/workouts/count", h)
        print(f"Status: {r.status_code}")
        # Mostra um trecho do corpo para diagnosticar (401 costuma vir com mensagem)
        body_preview = r.text[:500]
        print("Body (primeiros 500 chars):")
        print(indent(body_preview, "  "))

        if r.ok:
            ok_any = True
            print("\n✓ Autenticação OK nessa variante. Conteúdo completo:")
            print(indent(r.text, "  "))
            # 2) Se OK, testamos puxar 1 workout para validar o header também em /v1/workouts
            print("\n-> Testando /v1/workouts?limit=1...")
            r2 = try_request("/v1/workouts?limit=1", h)
            print(f"Status: {r2.status_code}")
            print("Body (primeiros 1000 chars):")
            print(indent(r2.text[:1000], "  "))
            break

    if not ok_any:
        print("\nX Todas as variantes retornaram erro.")
        print("Checklist para revisar:")
        print("  1) Sua conta precisa ser Hevy PRO.")
        print("  2) Gere a key em hevy.com (Settings > Developer) e copie sem espaços.")
        print("  3) Confirme se a env var HEVY_API_KEY está realmente setada neste shell.")
        print("  4) Se estiver usando .env, confirme que o carregamento está acontecendo (python-dotenv).")
        print("  5) Alguns endpoints podem exigir parâmetros; o /workouts/count é o mais simples para validar auth.")
        print("  6) Se o corpo da resposta trouxe pista de header esperado, ajuste o main.py conforme.")

if __name__ == "__main__":
    main()
#