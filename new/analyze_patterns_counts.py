import os
import pandas as pd
import xml.etree.ElementTree as ET
from collections import defaultdict
from utils.folders_paths import results_03_path, results_04_path

# Ensure output directory exists
os.makedirs(results_04_path, exist_ok=True)

print("Starting pattern analysis...")

# Data structures for counting patterns
evolution_patterns = defaultdict(lambda: defaultdict(int))  # {author_group: {pattern: count}}
change_patterns = defaultdict(lambda: defaultdict(int))     # {author_group: {pattern: count}}

# Process all XML files
for filename in os.listdir(results_03_path):
    if not filename.endswith(".xml"):
        continue
    
    file_path = os.path.join(results_03_path, filename)
    
    try:
        tree = ET.parse(file_path)
        root = tree.getroot()
    except ET.ParseError:
        print(f"Error parsing XML: {filename}")
        continue

    for version in root.findall(".//version"):
        raw_author = version.get("author")
        evolution = version.get("evolution")
        change = version.get("change")
        
        # Define author group
        if raw_author == "human":
            author_group = "human"
        else:
            author_group = "agent"
        
        # Count evolution patterns (skip None values which represent creation)
        if evolution and evolution != "None":
            evolution_patterns[author_group][evolution] += 1
        
        # Count change patterns (skip None values which represent creation)
        if change and change != "None":
            change_patterns[author_group][change] += 1

print("\n" + "="*60)
print("EVOLUTION PATTERNS COUNT")
print("="*60)

# Create Evolution Patterns DataFrame
evolution_rows = []
for author_group in sorted(evolution_patterns.keys()):
    patterns = evolution_patterns[author_group]
    row = {"Author": author_group}
    for pattern in ["Add", "Subtract", "Same"]:
        row[pattern] = patterns.get(pattern, 0)
    row["Total"] = sum(patterns.values())
    evolution_rows.append(row)

df_evolution = pd.DataFrame(evolution_rows)
print(df_evolution.to_string(index=False))

# Save Evolution Patterns
evolution_output = os.path.join(results_04_path, "evolution_patterns_counts.csv")
df_evolution.to_csv(evolution_output, index=False)
print(f"\n✓ Saved: {evolution_output}")

print("\n" + "="*60)
print("CHANGE PATTERNS COUNT")
print("="*60)

# Create Change Patterns DataFrame
change_rows = []
for author_group in sorted(change_patterns.keys()):
    patterns = change_patterns[author_group]
    row = {"Author": author_group}
    for pattern in ["Consistent", "Inconsistent", "Same"]:
        row[pattern] = patterns.get(pattern, 0)
    row["Total"] = sum(patterns.values())
    change_rows.append(row)

df_change = pd.DataFrame(change_rows)
print(df_change.to_string(index=False))

# Save Change Patterns
change_output = os.path.join(results_04_path, "change_patterns_counts.csv")
df_change.to_csv(change_output, index=False)
print(f"\n✓ Saved: {change_output}")

print("\n" + "="*60)
print("SUMMARY")
print("="*60)
print(f"Total evolution patterns found: {df_evolution['Total'].sum()}")
print(f"Total change patterns found: {df_change['Total'].sum()}")
print("="*60)
