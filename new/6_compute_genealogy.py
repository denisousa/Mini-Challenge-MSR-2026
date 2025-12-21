import os
import xml.etree.ElementTree as ET
import pandas as pd
from collections import defaultdict

def analyze_clone_creation(results_folder):
    """
    Parses all XML files in the specified folder to count clones created by
    humans and agents based on evolution and change patterns.
    
    Extracts metadata from filename structure: language_xxxx_org_project.xml
    """
    
    project_data = []
    
    # Data structures for counting patterns
    evolution_patterns = defaultdict(lambda: defaultdict(int))
    change_patterns = defaultdict(lambda: defaultdict(int))

    if not os.path.exists(results_folder):
        print(f"Error: The input directory '{results_folder}' does not exist.")
        return None, None, None

    # Iterate over files in the input folder
    for filename in os.listdir(results_folder):
        if filename.endswith(".xml"):
            file_path = os.path.join(results_folder, filename)
            
            # --- Filename Parsing Logic ---
            # Expected structure: language_xxxx_org_project.xml
            try:
                name_without_ext = os.path.splitext(filename)[0]
                parts = name_without_ext.split('_')
                
                if len(parts) >= 4:
                    project_name = parts[-1]
                    org_name = parts[-2]
                    language = parts[0]
                else:
                    # Fallback logic
                    project_name = name_without_ext
                    org_name = "Unknown"
                    language = "Unknown"
            except Exception:
                project_name = filename
                org_name = "Error"
                language = "Error"

            try:
                tree = ET.parse(file_path)
                root = tree.getroot()
                
                human_created_count = 0
                agent_created_count = 0
                
                # Analyze versions
                for version in root.findall(".//version"):
                    evolution = version.get("evolution")
                    change = version.get("change")
                    author = version.get("author")
                    
                    # Define author group
                    if author == "human":
                        author_group = "human"
                    else:
                        author_group = "agent"
                    
                    # Logic: Create (evolution="None" AND change="None")
                    if evolution == "None" and change == "None":
                        if author == "human":
                            human_created_count += 1
                        elif author == "agent":
                            agent_created_count += 1
                    else:
                        # Count evolution patterns (skip None values)
                        if evolution and evolution != "None":
                            evolution_patterns[author_group][evolution] += 1
                        
                        # Count change patterns (skip None values)
                        if change and change != "None":
                            change_patterns[author_group][change] += 1
                
                project_data.append({
                    "Language": language,
                    "Organization": org_name,
                    "Project": project_name,
                    "Human_Created": human_created_count,
                    "Agent_Created": agent_created_count,
                    "Total_Created": human_created_count + agent_created_count
                })

            except ET.ParseError:
                print(f"Warning: Could not parse {filename}. Skipping.")
            except Exception as e:
                print(f"Error processing {filename}: {e}")

    df = pd.DataFrame(project_data)
    
    if not df.empty:
        cols = ["Language", "Organization", "Project", "Human_Created", "Agent_Created", "Total_Created"]
        df = df[cols]
    
    # Create Combined Patterns DataFrame
    combined_rows = []
    all_authors = sorted(set(evolution_patterns.keys()) | set(change_patterns.keys()))
    
    for author_group in all_authors:
        evo_patterns = evolution_patterns[author_group]
        chg_patterns = change_patterns[author_group]
        
        # Get counts
        add_count = evo_patterns.get("Add", 0)
        subtract_count = evo_patterns.get("Subtract", 0)
        same_evo_count = evo_patterns.get("Same", 0)
        consistent_count = chg_patterns.get("Consistent", 0)
        inconsistent_count = chg_patterns.get("Inconsistent", 0)
        same_chg_count = chg_patterns.get("Same", 0)
        
        total_evo = sum(evo_patterns.values())
        total_chg = sum(chg_patterns.values())
        
        # Calculate percentages
        def pct(val, tot):
            return f"{(val/tot)*100:.2f}%" if tot > 0 else "0.00%"
        
        row = {
            "Author": author_group,
            "Add": pct(add_count, total_evo),
            "Subtract": pct(subtract_count, total_evo),
            "Same (Evolution)": pct(same_evo_count, total_evo),
            "Consistent": pct(consistent_count, total_chg),
            "Inconsistent": pct(inconsistent_count, total_chg),
            "Same (Change)": pct(same_chg_count, total_chg),
            "Total Evolution": total_evo,
            "Total Change": total_chg
        }
        combined_rows.append(row)
    
    df_patterns = pd.DataFrame(combined_rows)
        
    return df, df_patterns

# --- Execution ---

# 1. Define paths
input_folder = '03_results copy'
output_folder = '04_results'
output_filename = 'clone_creation_analysis.csv'

# 2. Create output directory if it doesn't exist
os.makedirs(output_folder, exist_ok=True)

print(f"Analyzing XML files in: {input_folder}...\n")

df_results, df_patterns = analyze_clone_creation(input_folder)

if df_results is not None and not df_results.empty:
    # 3. Calculate Totals for Console Output
    total_human = df_results['Human_Created'].sum()
    total_agent = df_results['Agent_Created'].sum()
    total_clones = df_results['Total_Created'].sum()

    print("--- Analysis per Project (Preview) ---")
    print(df_results.head(10).to_string(index=False))
    if len(df_results) > 10:
        print(f"... and {len(df_results) - 10} more rows.")
    
    print("\n" + "="*30)
    print("       FINAL SUMMARY       ")
    print("="*30)
    print(f"Total Projects          : {len(df_results)}")
    print(f"Total Created by HUMAN  : {total_human}")
    print(f"Total Created by AGENT  : {total_agent}")
    print(f"Grand Total Created     : {total_clones}")
    print("="*30)
    
    # 4. Save the CSV results to 04_results folder
    output_path = os.path.join(output_folder, output_filename)
    df_results.to_csv(output_path, index=False)
    print(f"\n[SUCCESS] Full results saved to: {output_path}")
    
    # 5. Display and save Combined Patterns
    if df_patterns is not None and not df_patterns.empty:
        print("\n" + "="*80)
        print("EVOLUTION AND CHANGE PATTERNS (Percentages)")
        print("="*80)
        print(df_patterns.to_string(index=False))
        patterns_output = os.path.join(output_folder, "patterns_analysis.csv")
        df_patterns.to_csv(patterns_output, index=False)
        print(f"\n✓ Saved: {patterns_output}")

else:
    print("No data found to save.")