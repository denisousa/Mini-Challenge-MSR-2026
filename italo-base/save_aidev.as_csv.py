import os
import pandas as pd

# === Read datasets ===
repo_df = pd.read_parquet("hf://datasets/hao-li/AIDev/repository.parquet")
pr_df = pd.read_parquet("hf://datasets/hao-li/AIDev/pull_request.parquet")
pr_commits = pd.read_parquet("hf://datasets/hao-li/AIDev/pr_commits.parquet")
pr_commit_details = pd.read_parquet("hf://datasets/hao-li/AIDev/pr_commit_details.parquet")

# === Create output folder ===
output_dir = "AiDev_Dataset"
os.makedirs(output_dir, exist_ok=True)

# === Export to CSV ===
repo_df.to_csv(os.path.join(output_dir, "repository.csv"), index=False)
pr_df.to_csv(os.path.join(output_dir, "pull_request.csv"), index=False)
pr_commits.to_csv(os.path.join(output_dir, "pr_commits.csv"), index=False)
pr_commit_details.to_csv(os.path.join(output_dir, "pr_commit_details.csv"), index=False)

