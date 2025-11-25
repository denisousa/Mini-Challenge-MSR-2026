import pandas as pd
from tqdm import tqdm
import configparser
import matplotlib.pyplot as plt
import requests
import pandas as pd

# === Read settings.ini ===
config = configparser.ConfigParser()
config.read("AiDev/settings.ini")
LANGUAGE = config["DETAILS"]["language_aidev"]
GITHUB_TOKEN = config["DETAILS"]["github_token"] 
HEADERS = {"Authorization": f"token {GITHUB_TOKEN}"}

print(f"Selected language: {LANGUAGE}")

# === Read datasets ===
repo_df = pd.read_parquet("hf://datasets/hao-li/AIDev/repository.parquet")
pr_df = pd.read_parquet("hf://datasets/hao-li/AIDev/pull_request.parquet")
pr_commits = pd.read_parquet("hf://datasets/hao-li/AIDev/pr_commits.parquet")
pr_commit_details = pd.read_parquet("hf://datasets/hao-li/AIDev/pr_commit_details.parquet")

# === Filter merged PRs ===
merged_prs = pr_df[pr_df["merged_at"].notna()].copy()

# === Join with repositories ===
merged_prs = merged_prs.merge(
    repo_df,
    left_on="repo_url",
    right_on="url",
    how="left",
    suffixes=("_pr", "_repo")
)

# === Keep only PRs in the specified language ===
valid_prs_lang = merged_prs[merged_prs["language"] == LANGUAGE].copy()

# === Get commits with at least one addition ===
commits_with_details = pr_commits.merge(
    pr_commit_details.drop(columns=["pr_id"]),
    on="sha",
    how="inner"
)

# === Keep commits that have additions (>0) ===
commits_with_additions = commits_with_details[commits_with_details["additions"] > 0]

# === Ensure unique commits per PR ===
commits_unique = commits_with_additions.drop_duplicates(subset=["sha", "pr_id"])

# === Filter commits that belong to PRs in the chosen language ===
commits_unique = commits_unique[commits_unique["pr_id"].isin(valid_prs_lang["id_pr"])]

# === Helper function: get PR base SHA via GitHub API ===
def get_base_sha(repo_url: str, pr_number: int) -> str:
    try:
        parts = repo_url.replace("https://github.com/", "").split("/")
        owner, repo = parts[4], parts[5]
        api_url = f"https://api.github.com/repos/{owner}/{repo}/pulls/{pr_number}"

        r = requests.get(api_url, headers=HEADERS)
        if r.status_code == 200:
            data = r.json()
            return data.get("base", {}).get("sha", None)
        else:
            print(f"Failed to fetch base SHA for {api_url} ({r.status_code})")
            return None
    except Exception as e:
        print(f"Error while getting base SHA: {e}")
        return None


# === Build final CSV ===
rows = []

for pr_id, group in tqdm(commits_unique.groupby("pr_id"), desc=f"Processing PRs ({LANGUAGE})"):
    group_sorted = group.sort_values("committed_at") if "committed_at" in group.columns else group

    pr_info = valid_prs_lang[valid_prs_lang["id_pr"] == pr_id]
    if pr_info.empty:
        continue  # extra safety

    repo_url = pr_info["repo_url"].values[0]
    pr_number = pr_info["number"].values[0]
    pr_html_url = pr_info["html_url"].values[0]
    pr_user = pr_info["user"].values[0]
    pr_agent = pr_info["agent"].values[0]
    pr_state = pr_info["state"].values[0]
    merged_at = pr_info["merged_at"].values[0]  # <-- merge date


    base_sha = get_base_sha(repo_url, pr_number)  # PR base SHA

    previous_sha = base_sha  # initial parent

    for i, (_, row) in enumerate(group_sorted.iterrows(), start=1):
        current_sha = row["sha"]

        rows.append({
            "id": f"{pr_number}_rev{i}",
            "number_pr": pr_number,
            "number_commit": i,
            "repo_url": repo_url,
            "user": pr_user,
            "agent": pr_agent,
            "state": pr_state,
            "merged_at": merged_at,
            "id_pr": pr_id,
            "sha_commit": current_sha,
            "url_commit": f"{repo_url}/commit/{current_sha}",
            "url_pr": pr_html_url,
            "child": current_sha
        })

        previous_sha = current_sha  # current becomes next parent

# === Create DataFrame and reorder columns ===
commit_df = pd.DataFrame(rows)
col_order = [
    "id",
    "number_pr",
    "number_commit",
    "repo_url",
    "user",
    "agent",
    "state",
    "merged_at",  # <-- new column
    "id_pr",
    "sha_commit",
    "url_commit",
    "url_pr",
    "parent",
    "child"
]
commit_df = commit_df[col_order]

# === Save final CSV ===
output_csv = f"AiDev/metadata/{LANGUAGE.lower()}_pr_commits_without_parents.csv"
commit_df.to_csv(output_csv, index=False)
print(f"CSV successfully generated: {output_csv} {commit_df.shape}")

# === Read generated CSV ===
df = pd.read_csv(output_csv)

# === Count commits per PR ===
commits_per_pr = df.groupby("id_pr")["sha_commit"].count().reset_index()
commits_per_pr.rename(columns={"sha_commit": "num_commits"}, inplace=True)

# === Basic statistics ===
print("\nGeneral statistics:")
print(commits_per_pr["num_commits"].describe())

# === Create boxplot ===
plt.figure(figsize=(10, 6))
plt.boxplot(
    commits_per_pr["num_commits"],
    vert=True,
    patch_artist=True,
    boxprops=dict(facecolor="skyblue", color="blue"),
    medianprops=dict(color="red"),
    whiskerprops=dict(color="blue"),
    capprops=dict(color="blue")
)
plt.title(f"Distribution of commits per PR ({LANGUAGE})")
plt.ylabel("Number of commits")
plt.xticks([1], ["PRs"])
plt.grid(axis="y", linestyle="--", alpha=0.7)
plt.savefig(f"AiDev/metadata/boxplot_{LANGUAGE.lower()}.png")
plt.show()

# === Split PR groups ===
single_commit_prs = commits_per_pr[commits_per_pr["num_commits"] == 1]
multi_commit_prs = commits_per_pr[commits_per_pr["num_commits"] > 1]

print(f"\nPRs with only 1 commit: {len(single_commit_prs)}")
print(f"PRs with more than 1 commit: {len(multi_commit_prs)}")

# === Save results ===
single_commit_prs.to_csv(f"AiDev/metadata/{LANGUAGE.lower()}_prs_single_commit.csv", index=False)
multi_commit_prs.to_csv(f"AiDev/metadata/{LANGUAGE.lower()}_prs_multi_commit.csv", index=False)

print("\nGenerated files:")
print(f"- {output_csv} (commits with parent/child and custom id)")
print(f"- AiDev/metadata/{LANGUAGE.lower()}_prs_single_commit.csv (PRs with 1 commit)")
print(f"- AiDev/metadata/{LANGUAGE.lower()}_prs_multi_commit.csv (PRs with >1 commit)")
