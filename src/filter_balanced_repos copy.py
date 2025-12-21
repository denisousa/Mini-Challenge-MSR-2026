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

# # Convert merged_at to datetime
# h_g_prs_merged['merged_at'] = pd.to_datetime(h_g_prs_merged['merged_at'])

# # Get the last merged PR date for each repository (only agent PRs)
# last_agent_merge = h_g_prs_merged[h_g_prs_merged['pr_type'] == 'agent'].groupby('full_name')['merged_at'].max().reset_index()
# last_agent_merge.columns = ['full_name', 'last_agent_merged_at']

# print(f"\nRepositories with agent PRs: {len(last_agent_merge)}")

# # Merge the last agent merge date back to the main dataframe
# h_g_prs_merged = pd.merge(h_g_prs_merged, last_agent_merge, on='full_name', how='left')

# # Filter PRs: keep only those merged on or before the last agent merge date
# print(f"\nBefore filtering by last agent merge date: {len(h_g_prs_merged)} PRs")
# h_g_prs_merged = h_g_prs_merged[h_g_prs_merged['merged_at'] <= h_g_prs_merged['last_agent_merged_at']]
# print(f"After filtering by last agent merge date: {len(h_g_prs_merged)} PRs")


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

# === Count total commits per repository ===
print("\nCounting commits per repository...")

# Merge pr_df with pr_commits to get commits per PR
merged_prs_with_commits = pd.merge(
    merged_prs,
    pr_commits_df[['pr_id']],
    how='inner',
    left_on='id_x',
    right_on='pr_id'
)

filtered_commits_data = []
for idx, row in filtered.iterrows():
    repo = row['full_name']
    print(f"  [{idx+1}/{len(filtered)}] Processing {repo}...")
    try:
        commits = list_commits_until(
            timestamp="2025-12-12T23:59:59Z",
            repo=repo,
            token=token
        )
        total_commits = len(commits)
        filtered_commits_data.append({
            'full_name': repo,
            'total_commits': total_commits
        })
        print(f"      ✓ Found {total_commits} commits")
    except Exception as e:
        print(f"      ✗ Error: {str(e)}")
        filtered_commits_data.append({
            'full_name': repo,
            'total_commits': 0
        })
    
    # Rate limiting
    import time
    time.sleep(1)

# Merge commit data
filtered_commits_df = pd.DataFrame(filtered_commits_data)
filtered = pd.merge(filtered, filtered_commits_df, on='full_name', how='left')
filtered['total_commits'] = filtered['total_commits'].fillna(0).astype(int)

# Calculate human commits
filtered['total_human_commits'] = filtered['total_commits'] - filtered['total_agent_commits']
filtered['total_human_commits'] = filtered['total_human_commits'].apply(lambda x: max(0, x))

# === Display results ===
print("\n" + "="*80)
print("FILTERED REPOSITORIES")
print("="*80)
print(filtered[['full_name', 'language', 'total_prs', 'agent_prs', 'human_prs', 
                'agent_percentage', 'human_percentage', 'stars', 'forks', 
                'total_commits', 'total_agent_commits', 'total_human_commits',
                'last_merged_pr_date']].to_string(index=False))

# === Save results ===
output_path = os.path.join(results_01_path, "balanced_repos_35_65.csv")
filtered.to_csv(output_path, index=False)

print(f"\n✓ Results saved to: {output_path}")
print(f"\nSummary:")
print(f"  - Total repositories found: {len(filtered)}")
if len(filtered) > 0:
    print(f"  - Min PRs: {filtered['total_prs'].min()}")
    print(f"  - Max PRs: {filtered['total_prs'].max()}")
    print(f"  - Avg PRs: {filtered['total_prs'].mean():.1f}")

# === Generate CSV with all merged PRs from filtered repositories ===
print("\n" + "="*80)
print("GENERATING CSV WITH ALL MERGED PRs FROM FILTERED REPOSITORIES")
print("="*80)

filtered_repo_names = filtered['full_name'].tolist()

# Get all agent PRs from filtered repositories
agent_prs_filtered = merged_prs[
    (merged_prs['full_name'].isin(filtered_repo_names)) & 
    (merged_prs['agent'].notna())
].copy()
agent_prs_filtered['pr_type'] = 'agent'

# Get all human PRs from filtered repositories
human_prs_filtered = human_prs_merged[
    human_prs_merged['repo_full_name'].isin(filtered_repo_names)
].copy()
human_prs_filtered = human_prs_filtered.rename(columns={'repo_full_name': 'full_name'})
human_prs_filtered['pr_type'] = 'human'

# Add language to human PRs
human_prs_filtered = pd.merge(
    human_prs_filtered,
    repo_df[['full_name', 'language']].drop_duplicates(),
    on='full_name',
    how='left'
)

# Get common columns
common_cols = ['number', 'title', 'user', 'state', 'created_at', 'closed_at', 
               'merged_at', 'full_name', 'language', 'agent', 'pr_type', 'html_url']

# Select columns that exist in both dataframes
agent_prs_export = agent_prs_filtered[common_cols]
human_prs_export = human_prs_filtered[common_cols]

# Combine both datasets
all_prs_filtered = pd.concat([agent_prs_export, human_prs_export], ignore_index=True)

# Sort by repository and created_at
all_prs_filtered = all_prs_filtered.sort_values(['full_name', 'created_at'])

print(f"Total merged PRs from filtered repositories: {len(all_prs_filtered)}")
print(f"  - Agent PRs: {len(agent_prs_export)}")
print(f"  - Human PRs: {len(human_prs_export)}")

# === Add total commits information per repository ===
print("\n" + "="*80)
print("GETTING TOTAL COMMITS PER REPOSITORY (until 2025-12-12)")
print("="*80)

# Get unique repositories from filtered list
unique_repos = all_prs_filtered['full_name'].unique()
repo_commits_data = []

for idx, repo in enumerate(unique_repos, 1):
    print(f"  [{idx}/{len(unique_repos)}] Processing {repo}...")
    try:
        commits = list_commits_until(
            timestamp="2025-12-12T23:59:59Z",
            repo=repo,
            token=token
        )
        total_commits = len(commits)
        repo_commits_data.append({
            'full_name': repo,
            'total_commits': total_commits
        })
        print(f"      ✓ Found {total_commits} commits")
    except Exception as e:
        print(f"      ✗ Error: {str(e)}")
        repo_commits_data.append({
            'full_name': repo,
            'total_commits': 0
        })
    
    # Rate limiting
    import time
    time.sleep(1)

# Create DataFrame with commit counts
repo_commits_df = pd.DataFrame(repo_commits_data)

# Merge with all_prs_filtered
all_prs_filtered = pd.merge(
    all_prs_filtered,
    repo_commits_df,
    on='full_name',
    how='left'
)

# Get agent commits per repository from filtered data
agent_commits_count = agent_prs_filtered.groupby('full_name').size().reset_index(name='total_agent_commits')

# Merge agent commits
all_prs_filtered = pd.merge(
    all_prs_filtered,
    agent_commits_count,
    on='full_name',
    how='left'
)

# Fill NaN values with 0
all_prs_filtered['total_commits'] = all_prs_filtered['total_commits'].fillna(0).astype(int)
all_prs_filtered['total_agent_commits'] = all_prs_filtered['total_agent_commits'].fillna(0).astype(int)

# Calculate human commits
all_prs_filtered['total_human_commits'] = all_prs_filtered['total_commits'] - all_prs_filtered['total_agent_commits']
all_prs_filtered['total_human_commits'] = all_prs_filtered['total_human_commits'].apply(lambda x: max(0, x))

print(f"\n✓ Commit information added to dataset")

# Save to CSV
prs_output_path = os.path.join(results_01_path, "balanced_repos_all_merged_prs.csv")
all_prs_filtered.to_csv(prs_output_path, index=False)

print(f"\n✓ All merged PRs saved to: {prs_output_path}")

# Show sample with new columns
print("\n=== Sample of merged PRs with commit info ===")
print(all_prs_filtered[['full_name', 'number', 'pr_type', 'agent', 'merged_at', 
                        'total_commits', 'total_agent_commits', 'total_human_commits']].head(10).to_string(index=False))