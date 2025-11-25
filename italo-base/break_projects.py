import os
import pandas as pd
from tqdm import tqdm
import configparser

# === Caminho do CSV principal ===
config = configparser.ConfigParser()
config.read("settings.ini")
LANGUAGE = config["DETAILS"]["language"]
csv_path = f"{LANGUAGE.lower()}_pr_commits_with_parents.csv"

# === Pasta de saída para os CSVs divididos ===
output_dir = "metadata"
os.makedirs(output_dir, exist_ok=True)

# === Ler CSV principal ===
df = pd.read_csv(csv_path)

# === Extrair nome do repositório a partir da URL da API ===
# Exemplo: https://api.github.com/repos/domaframework/doma → "doma"
df["repo_name"] = df["repo_url"].apply(lambda x: x.rstrip("/").split("/")[-1] if isinstance(x, str) else "unknown")

# === Agrupar por repositório ===
for repo_name, group in tqdm(df.groupby("repo_name"), desc="Gerando CSVs por repositório"):
    # Ordenar opcionalmente por número da PR e commit
    group_sorted = group.sort_values(["number_pr", "number_commit"])

    # Caminho de saída
    out_path = os.path.join(output_dir, f"{repo_name}.csv")

    # Salvar CSV individual
    group_sorted.to_csv(out_path, index=False)
    print(f"✅ Gerado: {out_path} ({len(group_sorted)} linhas)")

print("\n🎯 Todos os CSVs foram gerados em:", os.path.abspath(output_dir))
