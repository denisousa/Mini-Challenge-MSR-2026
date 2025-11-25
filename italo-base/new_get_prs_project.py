import os
import pandas as pd
from tqdm import tqdm
import configparser
from compute_time import timed
from boxplot import create_boxplot

# === Read settings.ini (only for token, language is now hard-coded) ===
config = configparser.ConfigParser()
config.read("AiDev/settings.ini")
GITHUB_TOKEN = config["DETAILS"].get("github_token", None)

# === Languages to process ===
# key -> used for folder / file naming
# value -> expected value in repo_df["language"]
LANGUAGES = {
    "c": "C",
    "csharp": "C#",
    "java": "Java",
    "php": "PHP",
    "ruby": "Ruby",
    "python": "Python",
}

print("Languages to process:", ", ".join(LANGUAGES.values()))

# === Read datasets ===
input_dir = "AiDev_Dataset"
repo_df = pd.read_csv(os.path.join(input_dir, "repository.csv"))
pr_df = pd.read_csv(os.path.join(input_dir, "pull_request.csv"))
pr_commits = pd.read_csv(os.path.join(input_dir, "pr_commits.csv"))
pr_commit_details = pd.read_csv(os.path.join(input_dir, "pr_commit_details.csv"))

# === Filter merged PRs ===
merged_prs = pr_df[pr_df["merged_at"].notna()].copy()

# === Join with repositories ===
merged_prs = merged_prs.merge(
    repo_df,
    left_on="repo_url",
    right_on="url",
    how="left",
    suffixes=("_pr", "_repo"),
)

# === Build unique commits table (for statistics and possible fallback) ===
commits_with_details = pr_commits.merge(
    pr_commit_details.drop(columns=["pr_id"]),
    on="sha",
    how="inner",
)

# Ensure unique commits per PR
commits_unique = commits_with_details.drop_duplicates(subset=["sha", "pr_id"])

# === Base output directory ===
base_metadata_dir = "AiDev/metadata"
os.makedirs(base_metadata_dir, exist_ok=True)

timed()
def main(lang_key, lang_name):
    print(f"\n=== Processing language: {lang_name} ===")

    # Create language-specific directory
    lang_dir = os.path.join(base_metadata_dir, lang_key)
    os.makedirs(lang_dir, exist_ok=True)

    # Filter PRs for this language
    valid_prs_lang = merged_prs[merged_prs["language"] == lang_name].copy()

    if valid_prs_lang.empty:
        print(f"No merged PRs found for language {lang_name}.")
        return

    # === Build table with ONLY the merged commit for each PR ===
    rows = []

    # Group by PR id to ensure 1 row per PR
    for pr_id, pr_group in tqdm(
        valid_prs_lang.groupby("id_pr"),
        desc=f"Building merged commit table ({lang_name})"
    ):
        pr_info = pr_group.iloc[0]

        repo_url = pr_info["repo_url"]
        pr_repo_name = pr_info["full_name"]
        pr_number = pr_info["number"]
        pr_html_url = pr_info["html_url"]
        pr_user = pr_info["user"]
        pr_agent = pr_info["agent"]
        pr_state = pr_info["state"]
        merged_at = pr_info["merged_at"]

        # Try to use merge_commit_sha from the PR dataset
        merged_sha = None
        if "merge_commit_sha" in valid_prs_lang.columns:
            merged_sha = pr_info.get("merge_commit_sha", None)

        # Fallback: if merge_commit_sha is missing/NaN, use the last commit from the PR
        if merged_sha is None or (isinstance(merged_sha, float) and pd.isna(merged_sha)):
            pr_commits_lang = commits_unique[commits_unique["pr_id"] == pr_id]
            if pr_commits_lang.empty:
                # No commits info; skip this PR
                continue

            if "committed_at" in pr_commits_lang.columns:
                pr_commits_lang = pr_commits_lang.sort_values("committed_at")

            merged_sha = pr_commits_lang.iloc[-1]["sha"]

        rows.append({
            "id": f"{pr_number}_merged",
            "repo_name": pr_repo_name,
            "number_pr": pr_number,
            "number_commit": 1,  # we only keep the merged commit
            "repo_url": repo_url,
            "user": pr_user,
            "agent": pr_agent,
            "state": pr_state,
            "merged_at": merged_at,
            "id_pr": pr_id,
            "sha_commit": merged_sha,
            "url_commit": f"{repo_url}/commit/{merged_sha}",
            "url_pr": pr_html_url,
            "parent": None,  # we are not tracking parents anymore
            "child": merged_sha,  # merged commit itself
        })

    if not rows:
        print(f"No rows generated for language {lang_name}.")
        return

    # === Create DataFrame for merged commits and reorder columns ===
    commit_df = pd.DataFrame(rows)
    col_order = [
        "id",
        "repo_name",
        "number_pr",
        "number_commit",
        "repo_url",
        "user",
        "agent",
        "state",
        "merged_at",
        "id_pr",
        "sha_commit",
        "url_commit",
        "url_pr",
        "parent",
        "child",
    ]
    commit_df = commit_df[col_order]

    # === Save merged-commit CSV for this language ===
    merged_csv = os.path.join(lang_dir, f"{lang_key}_pr_merged_commits.csv")
    commit_df.to_csv(merged_csv, index=False)
    print(f"Merged-commit CSV generated: {merged_csv} {commit_df.shape}")
    
    # ------------------------------------------------------------------
    #   STATISTICS (keep using ALL commits for these PRs, as before)
    # ------------------------------------------------------------------
    # Filter all commits that belong to PRs in this language
    lang_commit_mask = commits_unique["pr_id"].isin(valid_prs_lang["id_pr"])
    lang_commits = commits_unique[lang_commit_mask].copy()

    if lang_commits.empty:
        print(f"No commit details for language {lang_name} (statistics skipped).")
        return

    # Count commits per PR
    commits_per_pr = lang_commits.groupby("pr_id")["sha"].count().reset_index()
    commits_per_pr.rename(columns={"sha": "num_commits"}, inplace=True)

    # === Adicionar nome do projeto (repositório) e manter PR_ID + num_commits ===
    # Pega meta-informação das PRs (id_pr + nome do repo)
    pr_meta = (
        valid_prs_lang[["id_pr", "full_name"]]
        .drop_duplicates()
        .rename(columns={"id_pr": "pr_id", "full_name": "project_name"})
    )

    # Faz o merge para trazer o nome do projeto
    commits_per_pr = commits_per_pr.merge(pr_meta, on="pr_id", how="left")

    # Renomeia coluna de PR e organiza ordem das colunas
    commits_per_pr.rename(columns={"pr_id": "PR_ID"}, inplace=True)
    commits_per_pr = commits_per_pr[["project_name", "PR_ID", "num_commits"]]

    print("\nGeneral statistics (based on all commits in PRs):")
    print(commits_per_pr["num_commits"].describe())

    boxplot_path = create_boxplot(commits_per_pr, lang_dir, lang_name, lang_key)

    # === Split PR groups: only 1 commit vs >1 commit ===
    single_commit_prs = commits_per_pr[commits_per_pr["num_commits"] == 1].copy()
    multi_commit_prs = commits_per_pr[commits_per_pr["num_commits"] > 1].copy()

    # === Save statistics results ===
    single_commit_csv = os.path.join(lang_dir, f"{lang_key}_prs_single_commit.csv")
    multi_commit_csv = os.path.join(lang_dir, f"{lang_key}_prs_multi_commit.csv")

    print(f"\nPRs with only 1 commit ({lang_name}): {len(single_commit_prs)}")
    print(f"PRs with more than 1 commit ({lang_name}): {len(multi_commit_prs)}")

    single_commit_prs.to_csv(single_commit_csv, index=False)
    multi_commit_prs.to_csv(multi_commit_csv, index=False)

    print("\nGenerated files:")
    print(f"- {merged_csv} (one row per PR, with the merged commit)")
    print(f"- {single_commit_csv} (PRs with 1 commit total, with project_name/PR_ID/num_commits)")
    print(f"- {multi_commit_csv} (PRs with >1 commit total, with project_name/PR_ID/num_commits)")
    print(f"- {boxplot_path} (boxplot of commits per PR)")


for lang_key, lang_name in LANGUAGES.items():
    main(lang_key, lang_name)
    print("\nDone for all languages.")
