import os
import pandas as pd
import subprocess
import configparser

# === Read settings ===
config = configparser.ConfigParser()
config.read("metadata/dados/settings.ini")
LANGUAGE = config["DETAILS"]["language"]
print(f"Configured language: {LANGUAGE}")

# === Paths ===
base_dir = os.getcwd()
repos_base_dir = "git_repos"
input_csv = f"metadata/{LANGUAGE.lower()}_pr_commits_without_parents.csv"
output_csv = f"metadata/{LANGUAGE.lower()}_pr_commits_with_parents.csv"

# === Check if input CSV exists ===
if not os.path.exists(input_csv):
    raise FileNotFoundError(f"CSV file not found: {input_csv}")

# === Read CSV ===
df = pd.read_csv(input_csv)
repos_grouped = df.groupby("repo_url")

total_commits = len(df)
commit_counter = 0

# Initialize 'parent' column (if it does not exist yet)
if "parent" not in df.columns:
    df["parent"] = None

print(f"Processing {len(repos_grouped)} repositories and {total_commits} commits for {LANGUAGE}...\n")

# === Loop over repositories ===
for repo_url, group in repos_grouped:
    repo_name = repo_url.split("/")[-1].strip()
    repo_path = os.path.join(repos_base_dir, repo_name)

    if not os.path.exists(repo_path):
        print(f"[WARNING] Repository not found locally: {repo_path}, skipping.")
        continue

    os.chdir(repo_path)
    print(f"\nRepository: {repo_name} ({len(group)} commits)")

    for idx, row in group.iterrows():
        commit_sha = row["sha_commit"]

        try:
            # Fetch commit from remote (in case it's not present locally)
            subprocess.run(
                ["git", "fetch", "--all", "--quiet"],
                check=False
            )
            subprocess.run(
                ["git", "fetch", "origin", commit_sha],
                check=False
            )

            # Get the real parent of the commit
            result = subprocess.run(
                ["git", "rev-list", "--parents", "-n", "1", commit_sha],
                check=True,
                capture_output=True,
                text=True
            )
            output = result.stdout.strip().split()
            parent_sha = output[1] if len(output) > 1 else None

            df.loc[row.name, "parent"] = parent_sha

        except subprocess.CalledProcessError:
            print(f"Error while processing commit {commit_sha} in {repo_name}")
        except Exception as e:
            print(f"Unexpected error: {e}")

        commit_counter += 1
        print(f"[{commit_counter}/{total_commits}] commits processed", end="\r")

    os.chdir(base_dir)

# === Save updated CSV ===
df.to_csv(output_csv, index=False)
print(f"\nUpdated CSV saved to: {output_csv}")
