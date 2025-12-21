import os
import pandas as pd
import requests
from utils.languages import LANGUAGES
from utils.folders_paths import aidev_path, results_01_path
from dotenv import load_dotenv
from datetime import datetime
from typing import Any, Dict, List, Optional

load_dotenv()
token = os.getenv("GITHUB_TOKEN")

def list_commits_until(
    timestamp: str,
    repo: str,
    *,
    token: Optional[str] = None,
    per_page: int = 100,
    timeout: int = 30,
) -> List[Dict[str, Any]]:
    """Return commits on the default branch up to `timestamp` (inclusive)."""
    dt = datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
    until = dt.isoformat()

    session = requests.Session()
    session.headers.update({
        "Accept": "application/vnd.github+json",
        "User-Agent": "commits-until-script",
        **({"Authorization": f"Bearer {token}"} if token else {}),
    })

    url = f"https://api.github.com/repos/{repo}/commits"
    params = {"until": until, "per_page": per_page}

    commits: List[Dict[str, Any]] = []
    while url:
        r = session.get(url, params=params, timeout=timeout)
        r.raise_for_status()
        commits.extend(r.json())

        next_url = None
        for part in (r.headers.get("Link") or "").split(","):
            if 'rel="next"' in part:
                next_url = part.split(";")[0].strip().strip("<>")
                break

        url, params = next_url, None  # next_url already contains query params

    return commits


os.makedirs(results_01_path, exist_ok=True)

# === Load datasets ===
print("Loading datasets...")
agent_prs_df = pd.read_csv(os.path.join(results_01_path, "new_agent_pull_request.csv"))
human_prs_df = pd.read_csv(os.path.join(results_01_path, "new_human_pull_request.csv"))

# Concatenate both dataframes
h_g_prs_merged = pd.concat([agent_prs_df, human_prs_df], ignore_index=True)
print(f"Total merged PRs in agent dataset: {len(agent_prs_df)}")
print(f"Total merged PRs in human dataset: {len(human_prs_df)}")
print(f"Total merged PRs combined: {len(h_g_prs_merged)}")

# === Calculate repository statistics ===
print("\nCalculating repository statistics...")

# Count agent PRs per repository
agent_prs_count = h_g_prs_merged[h_g_prs_merged['pr_type'] == 'agent'].groupby('full_name').size().reset_index(name='agent_prs')

# Count human PRs per repository  
human_prs_count = h_g_prs_merged[h_g_prs_merged['pr_type'] == 'human'].groupby('full_name').size().reset_index(name='human_prs')

# Count total PRs per repository
total_prs_count = h_g_prs_merged.groupby('full_name').size().reset_index(name='total_prs')

# Merge all statistics
repo_stats = total_prs_count.copy()
repo_stats = pd.merge(repo_stats, agent_prs_count, on='full_name', how='left')
repo_stats = pd.merge(repo_stats, human_prs_count, on='full_name', how='left')

# Fill NaN values with 0
repo_stats['agent_prs'] = repo_stats['agent_prs'].fillna(0).astype(int)
repo_stats['human_prs'] = repo_stats['human_prs'].fillna(0).astype(int)

# Calculate percentages
repo_stats['agent_percentage'] = (repo_stats['agent_prs'] / repo_stats['total_prs'] * 100).round(2)
repo_stats['human_percentage'] = (repo_stats['human_prs'] / repo_stats['total_prs'] * 100).round(2)

print(f"Total repositories with merged PRs: {len(repo_stats)}")

# === Apply filters ===
# Filter 1: At least 60 PRs
filtered = repo_stats[repo_stats['total_prs'] >= 60].copy()
print(f"Repositories with at least 60 PRs: {len(filtered)}")

# Filter 2: Agent percentage between 35% and 65%
filtered = filtered[
    (filtered['agent_percentage'] >= 35) & 
    (filtered['agent_percentage'] <= 65)
].copy()

print(f"Repositories with agent PRs between 35% and 65%: {len(filtered)}")

# Sort by total PRs descending
filtered = filtered.sort_values('total_prs', ascending=False)
output_csv = os.path.join(results_01_path, "balanced_repositories.csv")
filtered.to_csv(output_csv, index=False)

# === Filter h_g_prs_merged to keep only repositories in filtered ===
print("\n" + "="*80)
print("FILTERING PRs TO KEEP ONLY BALANCED REPOSITORIES")
print("="*80)

filtered_repo_names = filtered['full_name'].tolist()

print(f"\nBefore filtering by repository list: {len(h_g_prs_merged)} PRs")
h_g_prs_merged_filtered = h_g_prs_merged[h_g_prs_merged['full_name'].isin(filtered_repo_names)].copy()
print(f"After filtering by repository list: {len(h_g_prs_merged_filtered)} PRs")

# Save filtered PRs to CSV
filtered_prs_output = os.path.join(results_01_path, "human_agent_pull_request.csv")
h_g_prs_merged_filtered.to_csv(filtered_prs_output, index=False)
print(f"\n✓ Filtered PRs saved to: {filtered_prs_output}")