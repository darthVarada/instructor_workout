import subprocess
import time
import webbrowser
import os
import signal

print("\n🚀 INICIANDO ORQUESTRAÇÃO DO INSTRUCTOR WORKOUT...\n")

processes = []

try:
    # ✅ 1. SUBIR MINIO
    print("🪣 Subindo MinIO...")
    minio_cmd = [
        "tools\\minio.exe",
        "server",
        "C:\\minio\\data",
        "--console-address",
        ":9001"
    ]
    p_minio = subprocess.Popen(minio_cmd)
    processes.append(p_minio)

    time.sleep(3)
    webbrowser.open("http://localhost:9001")
    print("✅ MinIO rodando em http://localhost:9001")

    # ✅ 2. GERAR DOCS DBT
    print("\n📚 Gerando documentação DBT...")
    os.chdir("dbt\\instructor_workout_dbt")
    subprocess.run(["dbt", "docs", "generate"])
    p_dbt = subprocess.Popen(["dbt", "docs", "serve"])
    processes.append(p_dbt)

    time.sleep(3)
    webbrowser.open("http://localhost:8080")
    print("✅ DBT Docs em http://localhost:8080")

    # ✅ 3. SUBIR O STREAMLIT
    print("\n🖥️ Subindo o Streamlit...")
    os.chdir("..\\..")
    p_streamlit = subprocess.Popen([
        "uv", "run", "streamlit", "run",
        "src/instructor_workout/streamlit_app/main.py"
    ])
    processes.append(p_streamlit)

    time.sleep(3)
    webbrowser.open("http://localhost:8501")
    print("✅ Streamlit em http://localhost:8501")

    print("\n✅ ORQUESTRAÇÃO FINALIZADA COM SUCESSO!")
    print("❗ Para encerrar tudo, pressione CTRL + C\n")

    while True:
        time.sleep(1)

except KeyboardInterrupt:
    print("\n🛑 Encerrando todos os serviços...")

    for p in processes:
        p.send_signal(signal.SIGTERM)

    print("✅ Tudo finalizado com segurança.")
