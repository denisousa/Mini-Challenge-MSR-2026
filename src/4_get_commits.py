import os
import pandas as pd
import requests
from utils.folders_paths import aidev_path, results_01_path
from dotenv import load_dotenv


def get_pr_last_commit(repo_full_name: str, pr_number: int, token: str) -> tuple:
    """Get the last commit SHA and author from a PR."""
    url = f"https://api.github.com/repos/{repo_full_name}/pulls/{pr_number}/commits"
    headers = {
        "Accept": "application/vnd.github+json",
        "Authorization": f"Bearer {token}",
        "User-Agent": "pr-commits-script"
    }
    
    try:
        response = requests.get(url, headers=headers, timeout=30)
        response.raise_for_status()
        commits = response.json()
        
        if commits:
            last_commit = commits[-1]  # Get the last commit
            sha = last_commit.get('sha')
            author = last_commit.get('commit', {}).get('author', {}).get('name', '')
            return sha, author
        return None, None
    except Exception as e:
        print(f"Error fetching commits for PR {pr_number} in {repo_full_name}: {e}")
        return None, None


def get_last_merged_pr_commit(repo_full_name: str, token: str) -> tuple:
    """
    Returns:
        (merge_commit_sha, pr_number, pr_language)
    """
    headers = {
        "Accept": "application/vnd.github+json",
        "Authorization": f"Bearer {token}",
        "User-Agent": "repo-pr-commits-script"
    }

    try:
        # 1. Get recently closed PRs
        pulls_url = f"https://api.github.com/repos/{repo_full_name}/pulls"
        pulls_resp = requests.get(
            pulls_url,
            headers=headers,
            params={
                "state": "closed",
                "sort": "updated",
                "direction": "desc",
                "per_page": 20
            },
            timeout=30
        )
        pulls_resp.raise_for_status()
        pulls = pulls_resp.json()

        merged_pr = None
        for pr in pulls:
            if pr.get("merged_at"):
                merged_pr = pr
                break

        if not merged_pr:
            return None, None, None

        merge_commit_sha = merged_pr.get("merge_commit_sha")
        pr_number = merged_pr.get("number")

        # 2. Get repository languages
        languages_url = f"https://api.github.com/repos/{repo_full_name}/languages"
        lang_resp = requests.get(languages_url, headers=headers, timeout=30)
        lang_resp.raise_for_status()
        languages = lang_resp.json()

        # Get dominant language (highest byte count)
        pr_language = max(languages, key=languages.get) if languages else None

        return merge_commit_sha, pr_number, pr_language

    except Exception as e:
        print(f"Error fetching last merged PR for repo {repo_full_name}: {e}")
        return None, None, None


load_dotenv()
token = os.getenv("GITHUB_TOKEN")
# sha, number, language = get_last_merged_pr_commit('microsoft/testfx', token)

os.makedirs(results_01_path, exist_ok=True)

# === Load datasets ===
print("Loading datasets...")
human_agent_prs_df = pd.read_csv(os.path.join(results_01_path, "human_agent_pull_request.csv"))
repo_df = pd.read_csv(os.path.join(aidev_path, "repository.csv"))
commits_df = pd.read_csv(os.path.join(aidev_path, "pr_commits.csv"))

# === Separate into human and agent dataframes ===
print("Separating PRs by type...")
human_prs_df = human_agent_prs_df[human_agent_prs_df['pr_type'] == 'human'].copy()
agent_prs_df = human_agent_prs_df[human_agent_prs_df['pr_type'] == 'agent'].copy()

print(f"Human PRs: {len(human_prs_df)}")
print(f"Agent PRs: {len(agent_prs_df)}")

# === Merge agent PRs with commits (only last commit per PR) ===
print("\nMerging agent PRs with commits...")
# Get only the last commit for each PR
last_commits_per_pr = commits_df.groupby('pr_id').last().reset_index()
agent_prs_with_commits = pd.merge(
    agent_prs_df,
    last_commits_per_pr[["pr_id", "sha", "author"]],
    how='inner',
    left_on='id',
    right_on='pr_id'
)
agent_prs_with_commits = agent_prs_with_commits.drop(columns=['pr_id'])

print(f"Agent PRs with last commit: {len(agent_prs_with_commits)}")

# === Get last commit for each human PR from GitHub ===
print("\nGetting last commit for each human PR from GitHub API...")
last_commits = []
total = len(human_prs_df)
i = 0
for _, row in human_prs_df.iterrows():
    i += 1
    if i % 10 == 0:
        print(f"Processing {i}/{total} PRs...")
    
    sha, author = get_pr_last_commit(row['full_name'], row['number'], token)
    last_commits.append({
        'id': row['id'],
        'sha': sha,
        'author': author
    })

last_commits_df = pd.DataFrame(last_commits)

# Merge back with human PRs
human_prs_with_commits = pd.merge(
    human_prs_df,
    last_commits_df,
    on='id',
    how='left'
)

print(f"Human PRs with last commit: {len(human_prs_with_commits)}")

# === Concatenate human and agent PRs ===
print("\nConcatenating human and agent PRs...")
all_prs_with_commits = pd.concat([human_prs_with_commits, agent_prs_with_commits], ignore_index=True)
print(f"Total PRs with commits: {len(all_prs_with_commits)}")

# === Get last commit for each repository ===
print("\nGetting last commit for each repository...")

# Get unique repositories
unique_repos = all_prs_with_commits[['full_name']].drop_duplicates()
print(f"Processing {len(unique_repos)} unique repositories...")

repo_last_commits = []
i = 0
for _, row in unique_repos.iterrows():
    i += 1
    print(f"Processing repository {i}/{len(unique_repos)}...")
    
    sha, number, language = get_last_merged_pr_commit(row['full_name'], token)
    repo_last_commits.append({
        'full_name': row['full_name'],
        'language': language,
        'number': int(number),
        'sha': sha,
        'author': author,
        'pr_type': 'human'
    })

repo_commits_df = pd.DataFrame(repo_last_commits)
print(f"Repository commits collected: {len(repo_commits_df)}")

# Add repository commits to the main dataframe
all_prs_with_commits = pd.concat([all_prs_with_commits, repo_commits_df], ignore_index=True)
all_prs_with_commits = all_prs_with_commits[['full_name', 'sha', 'author', 'pr_type', 'language', 'number']].drop_duplicates() 
print(f"Total records after adding repository commits: {len(all_prs_with_commits)}")

# === Save to CSV ===
output_csv = os.path.join(results_01_path, "human_agent_prs_with_commits.csv")
all_prs_with_commits.to_csv(output_csv, index=False)
print(f"\nSaved to: {output_csv}")

