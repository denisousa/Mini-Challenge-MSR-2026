import os
import pandas as pd
import xml.etree.ElementTree as ET
from collections import defaultdict
from utils.folders_paths import results_03_path, results_04_path

# Ensure output directory exists
os.makedirs(results_04_path, exist_ok=True)

# --- Data Structures ---

# 1. Stats by Language (Existing)
# stats[language][author_group]["change"][pattern] = count
stats_by_language = defaultdict(lambda: defaultdict(lambda: {
    "change": defaultdict(int),
    "evolution": defaultdict(int)
}))

# Totals for Language Percentage Calculation (Base 100% per Language)
language_grand_totals = defaultdict(lambda: {
    "change": 0,
    "evolution": 0
})

# 2. Stats by Specific Agent (New Request)
# stats_by_agent[agent_name]["change"][pattern] = count
stats_by_agent = defaultdict(lambda: {
    "change": defaultdict(int),
    "evolution": defaultdict(int)
})

print("Starting XML file processing...")

# --- Processing XML Files ---
for filename in os.listdir(results_03_path):
    if not filename.endswith(".xml"):
        continue
    
    # 1. Extract Metadata from Filename
    parts = filename.split('_')
    
    # Language (Index 0)
    try:
        language = parts[0]
    except IndexError:
        language = "Unknown"

    # Specific Agent Name (Index 1) - New Logic
    try:
        # File format is <Language>_<AgentName>_*.xml
        specific_agent_name = parts[1]
    except IndexError:
        specific_agent_name = "UnknownAgent"

    file_path = os.path.join(results_03_path, filename)
    try:
        tree = ET.parse(file_path)
        root = tree.getroot()
    except ET.ParseError:
        print(f"Error parsing XML: {filename}")
        continue

    # 2. Iterate Versions
    for version in root.findall(".//version"):
        raw_author = version.get("author")
        evolution = version.get("evolution") 
        change = version.get("change")       
        
        # --- Logic A: For Language Table (Generic Grouping) ---
        if raw_author == "Developer":
            author_group_generic = "Developer"
        else:
            author_group_generic = "Agent" # Generic bucket
        
        # Update Language Stats
        if change:
            stats_by_language[language][author_group_generic]["change"][change] += 1
            language_grand_totals[language]["change"] += 1
        if evolution:
            stats_by_language[language][author_group_generic]["evolution"][evolution] += 1
            language_grand_totals[language]["evolution"] += 1

        # --- Logic B: For Agent Table (Specific Grouping) ---
        if raw_author == "Developer":
            # We aggregate all developers into one global "Developer" bucket
            # to serve as a baseline comparison for all agents
            agent_key = "Developer"
        else:
            # Here we use the specific name from the filename (e.g., "Devin")
            agent_key = specific_agent_name
        
        # Update Agent Stats (Global across languages)
        if change:
            stats_by_agent[agent_key]["change"][change] += 1
        if evolution:
            stats_by_agent[agent_key]["evolution"][evolution] += 1

# ==========================================
# OUTPUT 1: Table by Language (Your previous table)
# ==========================================
rows_language = []

for language, authors_data in stats_by_language.items():
    total_change_lang = language_grand_totals[language]["change"]
    total_evolution_lang = language_grand_totals[language]["evolution"]

    for author, types in authors_data.items():
        # Helper for % relative to Language Total
        def calc_pct_lang(dictionary, key, grand_total):
            if grand_total > 0:
                return (dictionary[key] / grand_total) * 100
            return 0.0

        rows_language.append({
            "Name": language,
            "Set": author,
            "Consistent": f"{calc_pct_lang(types['change'], 'Consistent', total_change_lang):.2f}%",
            "Inconsistent": f"{calc_pct_lang(types['change'], 'Inconsistent', total_change_lang):.2f}%",
            "Same (Change)": f"{calc_pct_lang(types['change'], 'Same', total_change_lang):.2f}%",
            "Add": f"{calc_pct_lang(types['evolution'], 'Add', total_evolution_lang):.2f}%",
            "Subtract": f"{calc_pct_lang(types['evolution'], 'Subtract', total_evolution_lang):.2f}%",
            "Same (Evolution)": f"{calc_pct_lang(types['evolution'], 'Same', total_evolution_lang):.2f}%"
        })

columns_order = ["Name", "Set", "Consistent", "Inconsistent", "Same (Change)", "Add", "Subtract", "Same (Evolution)"]
df_language = pd.DataFrame(rows_language, columns=columns_order).sort_values(by=["Name", "Set"])

output_csv_lang = os.path.join(results_04_path, "evolution_change_by_language.csv")
df_language.to_csv(output_csv_lang, index=False)

print(f"Table 1 (By Language) saved at: {output_csv_lang}")
print(df_language.to_markdown(index=False))
print("\n" + "="*80 + "\n")

# ==========================================
# OUTPUT 2: Table by Specific Agent (Global)
# ==========================================
rows_agent = []

for agent_name, types in stats_by_agent.items():
    
    # Calculate Totals for this specific Agent across all languages
    # This allows us to see the "Genealogy Profile" of the agent (e.g., How often DOES Devin break things?)
    total_change_agent = sum(types["change"].values())
    total_evolution_agent = sum(types["evolution"].values())

    def calc_pct_agent(dictionary, key, total):
        if total > 0:
            return (dictionary[key] / total) * 100
        return 0.0

    rows_agent.append({
        "Agent Name": agent_name,
        # Change Patterns
        "Consistent": f"{calc_pct_agent(types['change'], 'Consistent', total_change_agent):.2f}%",
        "Inconsistent": f"{calc_pct_agent(types['change'], 'Inconsistent', total_change_agent):.2f}%",
        "Same (Change)": f"{calc_pct_agent(types['change'], 'Same', total_change_agent):.2f}%",
        # Evolution Patterns
        "Add": f"{calc_pct_agent(types['evolution'], 'Add', total_evolution_agent):.2f}%",
        "Subtract": f"{calc_pct_agent(types['evolution'], 'Subtract', total_evolution_agent):.2f}%",
        "Same (Evolution)": f"{calc_pct_agent(types['evolution'], 'Same', total_evolution_agent):.2f}%",
        # Raw counts (Optional, good for debugging/checking sample size)
        "Total Changes": total_change_agent
    })

# Create DataFrame
columns_agent_order = [
    "Agent Name", 
    "Consistent", "Inconsistent", "Same (Change)", 
    "Add", "Subtract", "Same (Evolution)",
    "Total Changes"
]

df_agent = pd.DataFrame(rows_agent, columns=columns_agent_order)

# Sort: Developer first, then others alphabetically
df_agent["is_dev"] = df_agent["Agent Name"] == "Developer"
df_agent = df_agent.sort_values(by=["is_dev", "Agent Name"], ascending=[False, True]).drop(columns=["is_dev"])

output_csv_agent = os.path.join(results_04_path, "evolution_change_by_agent_global.csv")
df_agent.to_csv(output_csv_agent, index=False)

print(f"Table 2 (By Specific Agent - Global) saved at: {output_csv_agent}")
print("\n--- Global Agent Genealogy Comparison ---")
print(df_agent.to_markdown(index=False))