cd C:\Users\69pctechops\instructor_workout

# Sobe Docker em outra janela (ou em background)
Start-Process powershell -ArgumentList "cd C:\Users\69pctechops\instructor_workout; docker compose up" -WindowStyle Minimized

# Espera um tempo pros containers subirem (ajusta se precisar)
Start-Sleep -Seconds 30

# Abre as páginas no navegador padrão
Start-Process "http://localhost:8080"  # Airflow
Start-Process "http://localhost:8501"  # Streamlit
Start-Process "http://localhost:8081"  # dbt docs
