
import matplotlib.pyplot as plt

def create_boxplot(commits_per_pr, lang_dir, lang_name, lang_key):
    # === Create boxplot ===
    plt.figure(figsize=(10, 6))
    plt.boxplot(
        commits_per_pr["num_commits"],
        vert=True,
        patch_artist=True,
        boxprops=dict(facecolor="skyblue", color="blue"),
        medianprops=dict(color="red"),
        whiskerprops=dict(color="blue"),
        capprops=dict(color="blue"),
    )
    plt.title(f"Distribution of commits per PR ({lang_name})")
    plt.ylabel("Number of commits")
    plt.xticks([1], ["PRs"])
    plt.grid(axis="y", linestyle="--", alpha=0.7)

    boxplot_path = os.path.join(lang_dir, f"boxplot_{lang_key}.png")
    plt.savefig(boxplot_path)
    plt.close()
    print(f"Boxplot saved to: {boxplot_path}")
    return boxplot_path
