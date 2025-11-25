import os
import pandas as pd
import xml.etree.ElementTree as ET
import configparser
from tqdm import tqdm

# === Ler configurações ===
config = configparser.ConfigParser()
config.read("./metadata/dados/settings.ini")

projects = [p.strip() for p in config.get("DETAILS", "projects").split(",")]
path_to_repo = config.get("DETAILS", "path_to_repo", fallback=".")
search_results_dir = os.path.join(path_to_repo, "search_results")
metadata_dir = os.path.join(path_to_repo, "metadata")

os.makedirs(search_results_dir, exist_ok=True)

def extract_fingerprints(xml_path):
    if not os.path.exists(xml_path):
        return set()

    try:
        # Lê o arquivo inteiro como texto
        with open(xml_path, "r", encoding="utf-8", errors="ignore") as f:
            content = f.read()

        # Achar onde o XML começa (primeiro "<")
        idx = content.find("<")
        if idx > 0:
            content = content[idx:]  # remove cabeçalho

        # Agora parsear como XML real
        root = ET.fromstring(content)

        fingerprints = set()

        # Encontrar nós <set> com fingerprint
        for s in root.findall(".//set"):
            fp = s.attrib.get("fingerprint")
            if not fp:
                continue

            blocks = s.findall("block")
            files = [b.attrib.get("sourceFile", "").lower() for b in blocks]

            # Ignore se TODOS os blocos são testes
            if all(("test" in f or "tests" in f or "spec" in f) for f in files):
                continue

            fingerprints.add(fp)

        return fingerprints

    except Exception as e:
        print(f"⚠️ Erro ao ler XML {xml_path}: {e}")
        return set()

# === Loop por projeto ===
for project in projects:
    csv_path = os.path.join(metadata_dir, f"{project}.csv")
    if not os.path.exists(csv_path):
        print(f"⚠️ CSV não encontrado: {csv_path}")
        continue

    df = pd.read_csv(csv_path)

    if df.empty:
        print(f"⚠️ CSV vazio: {csv_path}")
        continue

    print(f"\n📌 Processando projeto: {project}")

    results = []

    # Agrupar por PR
    for pr_id, pr_group in tqdm(df.groupby("number_pr"), desc=f"PRs - {project}"):
        pr_group = pr_group.sort_values("number_commit")
        total_commits = pr_group.shape[0]

        active_clones = {}  # fingerprint -> commit_start

        for _, row in pr_group.iterrows():
            number_commit = row["number_commit"]

            xml_parent = os.path.join(search_results_dir, f"simian-result-{project}-{pr_id}-{number_commit}-parent.xml")
            xml_child = os.path.join(search_results_dir, f"simian-result-{project}-{pr_id}-{number_commit}-child.xml")

            parent_clones = extract_fingerprints(xml_parent)
            child_clones = extract_fingerprints(xml_child)

            # Clones novos introduzidos neste commit
            new_clones = child_clones - parent_clones
            # Clones que desapareceram
            disappeared = set(active_clones.keys()) - child_clones

            # Registrar desaparecidos
            for fp in disappeared:
                results.append({
                    "project": project,
                    "pr": pr_id,
                    "clone_fingerprint": fp,
                    "start_commit": active_clones[fp],
                    "end_commit": number_commit - 1,
                    "total_commits_in_pr": total_commits
                })
                del active_clones[fp]

            # Registrar novos ativos
            for fp in new_clones:
                active_clones[fp] = number_commit

        # Finalizar clones que chegaram até o último commit
        for fp, start_commit in active_clones.items():
            results.append({
                "project": project,
                "pr": pr_id,
                "clone_fingerprint": fp,
                "start_commit": start_commit,
                "end_commit": pr_group["number_commit"].max(),
                "total_commits_in_pr": total_commits
            })

    # === Salvar resultado do projeto ===
    output_csv = os.path.join(metadata_dir, f"{project}_clone_lifetimes.csv")
    pd.DataFrame(results).to_csv(output_csv, index=False)
    print(f"✅ CSV salvo: {output_csv}")
