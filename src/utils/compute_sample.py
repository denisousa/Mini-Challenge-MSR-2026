import math

def compute_sample(merged_prs):
    # Population size (number of merged PRs)
    N = len(merged_prs)

    # Parameters
    Z = 1.96        # 95% confidence
    p = 0.5         # maximum variability
    E = 0.05        # 5% margin of error

    # Initial sample size without finite population correction
    n0 = (Z**2 * p * (1 - p)) / (E**2)

    # Apply finite population correction
    n = n0 / (1 + (n0 - 1) / N)

    # Round up
    sample_size = math.ceil(n)

    print(f"Sample size needed: {sample_size} rows")

    # Actual sampling
    return merged_prs.sample(sample_size, random_state=42)
