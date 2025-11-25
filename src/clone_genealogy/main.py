import os
import pandas as pd
from tqdm import tqdm
import subprocess
import requests
import configparser
from core import get_clone_genealogy

# === Read settings.ini ===
config = configparser.ConfigParser()
config.read("AiDev/settings.ini")
LANGUAGE = config["DETAILS"]["language_aidev"]
GITHUB_TOKEN = config["DETAILS"]["github_token"] 
HEADERS = {"Authorization": f"token {GITHUB_TOKEN}"}

print(f"Selected language: {LANGUAGE}")

# === Paths ===
csv_path = f"AiDev/metadata/{LANGUAGE.lower()}_pr_commits_without_parents.csv"
output_dir = "AiDev/git_repos"

# === Create output folder ===
os.makedirs(output_dir, exist_ok=True)

# === Read CSV ===
if not os.path.exists(csv_path):
    raise FileNotFoundError(f"CSV file not found: {csv_path}")

df = pd.read_csv(csv_path)

# === Get unique repositories ===
repos = df["repo_url"].dropna().unique()
print(f"{len(repos)} unique repositories found for {LANGUAGE}.")

def get_clone_url(api_url: str) -> str | None:
    """
    Given a repository API link (e.g. https://api.github.com/repos/domaframework/doma),
    return the 'clone_url' field from the JSON.
    """
    try:
        r = requests.get(api_url, headers=HEADERS, timeout=15)
        if r.status_code == 200:
            data = r.json()
            return data.get("clone_url", None)
        else:
            print(f"Failed to get clone_url ({r.status_code}) for {api_url}")
            return None
    except Exception as e:
        print(f"Error fetching clone_url for {api_url}: {e}")
        return None


# === Clone repositories ===
def main():
    for api_url in tqdm(repos, desc=f"Cloning repositories ({LANGUAGE})"):
        clone_url = get_clone_url(api_url)
        if not clone_url:
            print(f"Could not get clone_url for {api_url}")
            continue

        repo_name = clone_url.split("/")[-1].replace(".git", "")
        repo_path = os.path.join(output_dir, repo_name)

        if os.path.exists(repo_path):
            print(f"Repository '{repo_name}' already exists, skipping.")
            continue

        try:
            subprocess.run(["git", "clone", clone_url, repo_path], check=True)
            print(f"Cloned: {repo_name}")
        except subprocess.CalledProcessError as e:
            print(f"Error cloning {repo_name}: {e}")
        except Exception as e:
            print(f"Unexpected error with {repo_name}: {e}")
