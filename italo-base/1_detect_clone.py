import os
import pandas as pd
import subprocess
import configparser
from tqdm import tqdm
from nicad_operations import run_nicad

# === Read settings ===
config = configparser.ConfigParser()
config.read("./AiDev/settings.ini")

projects = [p.strip() for p in config.get("DETAILS", "projects").split(",")]
path_to_repo = config.get("DETAILS", "path_to_repo", fallback=".")
search_results_dir = os.path.join(path_to_repo, "search_results")
language = config.get("DETAILS", "language")

os.makedirs(search_results_dir, exist_ok=True)

# === Function to run Simian ===
def run_simian(repo_path, project, number_pr, number_commit, mode):
    """
    mode = 'parent' or 'child'
    """
    output_xml = os.path.join(
        search_results_dir,
        f"simian-result-{project}-{number_pr}-{number_commit}-{mode}.xml"
    )
    simian_jar = os.path.join(path_to_repo, "simian-4.0.0", "simian-4.0.0.jar")
    options = "-formatter=xml -threshold=6"
    simian_command = (
        f'java -jar "{simian_jar}" {options} '
        f'-includes="{repo_path}/**/*.{language}" > "{output_xml}"'
    )
    os.system(simian_command)
    print(f"✅ Simian result saved to {output_xml}")


# === Loop over projects ===
for project in projects:
    metadata_csv = os.path.join(path_to_repo, "metadata", f"{project}.csv")
    repo_path = os.path.join(path_to_repo, "git_repos", project)

    if not os.path.exists(metadata_csv):
        print(f"⚠️ CSV not found: {metadata_csv}")
        continue
    if not os.path.exists(repo_path):
        print(f"⚠️ Repository not found: {repo_path}")
        continue

    df = pd.read_csv(metadata_csv)
    if df.empty:
        print(f"⚠️ Empty CSV: {metadata_csv}")
        continue

    print(f"\n📦 Processing project: {project} ({len(df)} commits)")

    os.chdir(repo_path)

    for _, row in tqdm(df.iterrows(), total=len(df), desc=f"Commits of {project}"):

        number_pr = row["number_pr"]
        number_commit = row["number_commit"]

        parent_sha = str(row["parent"]).strip()
        child_sha = str(row["child"]).strip()

        # --- Run Simian on parent ---
        if parent_sha and parent_sha != "None" and len(parent_sha) > 5:
            try:
                subprocess.run(
                    ["git", "checkout", parent_sha],
                    check=True,
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL
                )
                result_path = os.path.join(
                    search_results_dir,
                    f"simian-result-{project}-{number_pr}-{number_commit}-parent.xml"
                )
                run_nicad(repo_path, result_path)

            except subprocess.CalledProcessError:
                print(f"⚠️ Failed to checkout parent {parent_sha} ({project} PR {number_pr})")

        # --- Run Simian on child ---
        if child_sha and len(child_sha) > 5:
            try:
                subprocess.run(
                    ["git", "checkout", child_sha],
                    check=True,
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL
                )
                result_path = os.path.join(
                    search_results_dir,
                    f"simian-result-{project}-{number_pr}-{number_commit}-child.xml"
                )
                run_nicad(repo_path, result_path)

            except subprocess.CalledProcessError:
                print(f"⚠️ Failed to checkout child {child_sha} ({project} PR {number_pr})")

print("\n✅ Execution completed successfully!")
