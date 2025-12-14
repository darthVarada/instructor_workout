# Colar este bloco no final de notebooks/01_schema_dictionary.ipynb e executar
from IPython.display import display, Markdown
import pandas as pd, json

display(Markdown("## Página de Metadados — silver (users & depara)"))

# users_silver_df
display(Markdown("### `users_silver_df`"))
if 'users_silver_df' in globals():
    df = users_silver_df
    try:
        display(Markdown(f"**Dimensão:** {df.shape[0]} linhas × {df.shape[1]} colunas"))
    except Exception:
        pass
    display(df)
    display(Markdown("**Dtypes:**"))
    display(pd.DataFrame(df.dtypes.astype(str), columns=['dtype']))
else:
    display(Markdown("_Variável `users_silver_df` não encontrada no kernel._"))

# silver_users_dict
display(Markdown("### `silver_users_dict`"))
if 'silver_users_dict' in globals():
    su = silver_users_dict
    try:
        summary = pd.DataFrame([
            {'arquivo': k, 'num_colunas': v.get('num_colunas'), 's3_path': v.get('s3_path')}
            for k, v in su.items()
        ])
        display(summary)
    except Exception:
        pass
    display(Markdown("**Conteúdo completo (JSON):**"))
    display(Markdown("```json\n" + json.dumps(su, ensure_ascii=False, indent=2) + "\n```"))
else:
    display(Markdown("_Variável `silver_users_dict` não encontrada no kernel._"))

# silver_depara_dict
display(Markdown("### `silver_depara_dict`"))
if 'silver_depara_dict' in globals():
    sd = silver_depara_dict
    try:
        summary = pd.DataFrame([
            {'arquivo': k, 'num_colunas': v.get('num_colunas'), 's3_path': v.get('s3_path')}
            for k, v in sd.items()
        ])
        display(summary)
    except Exception:
        pass
    display(Markdown("**Conteúdo completo (JSON):**"))
    display(Markdown("```json\n" + json.dumps(sd, ensure_ascii=False, indent=2) + "\n```"))
else:
    display(Markdown("_Variável `silver_depara_dict` não encontrada no kernel._"))