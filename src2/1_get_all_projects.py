import os
import pandas as pd
from utils.compute_sample import compute_sample
from utils.languages import LANGUAGES
from utils.folders_paths import aidev_path, rq1_path
from utils.boxplot import main_individual_analysis, export_outlier_projects_csv, export_q3plus_projects_csv, enrich_projects_with_github_counts_until_date

os.makedirs(rq1_path, exist_ok=True)

# === Load datasets ===
repo_df = pd.read_csv(os.path.join(aidev_path, "repository.csv"))
pr_df = pd.read_csv(os.path.join(aidev_path, "pull_request.csv"))

# === Remove duplicate repositories and filter by language ===
repo_df = repo_df.drop_duplicates(subset="url", keep="first")
repo_df = repo_df[repo_df["language"].isin(LANGUAGES.keys())]

# === Keep only merged pull requests ===
merged_prs = pr_df[pr_df["merged_at"].notna()].copy()

# === Join PRs with repositories to retrieve project names and languages ===
merged_prs = merged_prs.merge(
    repo_df[["url", "full_name", "language"]],
    left_on="repo_url",
    right_on="url",
    how="left"
)

unique_repos_count = merged_prs["url"].nunique()
print(f"Unique repositories by language in AIDev: {unique_repos_count}")

# === Count merged PRs per project (grouped by language) ===
merged_prs_per_project = (
    merged_prs.groupby(["full_name", "language"])
    .size()
    .reset_index(name="num_prs")
    .sort_values(by=["language", "num_prs"], ascending=[True, False])
)

# Generate Boxplot per languague
merged_prs["merged_at"] = pd.to_datetime(merged_prs["merged_at"], errors="coerce")

latest_pr_per_repo = (
    merged_prs
    .dropna(subset=["full_name", "language", "merged_at"])
    .sort_values("merged_at")
    .groupby(["full_name", "language"], as_index=False)
    .tail(1)[["full_name", "language", "merged_at"]]
    .rename(columns={"merged_at": "latest_merged_at"})
)

# Outliers
outliers_projects = export_outlier_projects_csv(merged_prs_per_project)
outliers_projects = outliers_projects.merge(latest_pr_per_repo, on=["full_name", "language"], how="left")
outliers_projects = enrich_projects_with_github_counts_until_date(outliers_projects, date_col="latest_merged_at")
outliers_outpath = os.path.join(rq1_path, "outlier_projects_by_language.csv")
outliers_projects.to_csv(outliers_outpath, index=False)

# Q3+
q3plus_projects = export_q3plus_projects_csv(merged_prs_per_project)   
q3plus_projects = q3plus_projects.merge(latest_pr_per_repo, on=["full_name", "language"], how="left")
q3plus_projects = enrich_projects_with_github_counts_until_date(q3plus_projects, date_col="latest_merged_at")
q3plus_outpath = os.path.join(rq1_path, "q3plus_projects_by_language.csv")
q3plus_projects.to_csv(q3plus_outpath, index=False)

print("\n=== Number of MERGED PRs per project (grouped by language) ===")
print(merged_prs_per_project.head())

# === Aggregate: number of projects and total merged PRs per language ===
language_summary = (
    merged_prs_per_project
    .groupby("language")
    .agg(
        num_projects=("full_name", "nunique"),
        total_prs=("num_prs", "sum")
    )
    .reset_index()
    .sort_values("language")
)

print("\n=== Summary: number of projects and total MERGED PRs per language ===")
print(language_summary)

# === Save CSV (full dataset) ===
output_path = "rq1/projects_merged_prs_rq1.csv"
language_summary.to_csv(output_path, index=False)
print(f"\nCSV saved as: {output_path}")

