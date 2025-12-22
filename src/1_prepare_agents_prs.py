import os
import pandas as pd
from utils.languages import LANGUAGES
from utils.folders_paths import aidev_path, results_01_path
from dotenv import load_dotenv

load_dotenv()
token = os.getenv("GITHUB_TOKEN")
os.makedirs(results_01_path, exist_ok=True)

# === Load datasets ===
print("Loading datasets...")
repo_df = pd.read_csv(os.path.join(aidev_path, "repository.csv"))
pr_df = pd.read_csv(os.path.join(aidev_path, "pull_request.csv"))

repo_df = repo_df[repo_df["language"].isin(LANGUAGES.keys())]
pr_df_merged = pr_df[pr_df["merged_at"].notna()].copy()
pr_df_merged['pr_type'] = 'agent'

# merged_prs = pd.merge(
#     pr_df_merged,              
#     repo_df[["id", "url", "full_name", "language", "stars", "forks"]],
#     how='inner',        # 'inner' keeps only rows that match in both tables
#     left_on='repo_id',  # Column name in pr_df
#     right_on='id'       # Column name in repo_df
# )

merged_prs = pr_df_merged.merge(
    repo_df,
    left_on="repo_url",
    right_on="url",
    how="left",
    suffixes=("_pr", "_repo"),
)

# Rename id_x to id and remove id_y
# merged_prs = merged_prs.rename(columns={'id_x': 'id'})
# merged_prs = merged_prs.drop(columns=['id_y'])

output_csv = os.path.join(results_01_path, "new_agent_pull_request.csv")
merged_prs.to_csv(output_csv, index=False)