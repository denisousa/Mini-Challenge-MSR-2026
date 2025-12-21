import os
import pandas as pd
import xml.etree.ElementTree as ET
from collections import defaultdict
from utils.folders_paths import results_03_path, results_04_path


results_03_path = os.path.abspath("03_results copy")
# Ensure output directory exists
os.makedirs(results_04_path, exist_ok=True)

print("Starting analysis of agent-created clones modifications...")

def analyze_agent_clones_modifications(results_folder):
    """
    For each project, analyzes modifications made to clones created by agents.
    Tracks who (human or agent) performs Consistent, Inconsistent, Add, Subtract operations
    on clones that were originally created by agents.
    Excludes "Same" patterns from the count.
    """
    
    project_results = []
    
    if not os.path.exists(results_folder):
        print(f"Error: The input directory '{results_folder}' does not exist.")
        return None
    
    # Iterate over files in the input folder
    for filename in os.listdir(results_folder):
        if not filename.endswith(".xml"):
            continue
        
        file_path = os.path.join(results_folder, filename)
        
        # Extract project name from filename
        try:
            name_without_ext = os.path.splitext(filename)[0]
            parts = name_without_ext.split('_')
            
            if len(parts) >= 4:
                project_name = f"{parts[-2]}_{parts[-1]}"
                language = parts[0]
            else:
                project_name = name_without_ext
                language = "Unknown"
        except Exception:
            project_name = filename
            language = "Unknown"
        
        # Track patterns by who modified agent-created clones in this project
        agent_clone_modifications = {
            "human": {"Consistent": 0, "Inconsistent": 0, "Add": 0, "Subtract": 0},
            "agent": {"Consistent": 0, "Inconsistent": 0, "Add": 0, "Subtract": 0}
        }
        
        has_agent_clones = False  # Track if project has any agent-created clones
        
        try:
            tree = ET.parse(file_path)
            root = tree.getroot()
            
            # Process each lineage (clone genealogy) separately
            for lineage in root.findall(".//lineage"):
                versions = lineage.findall("version")
                
                if not versions:
                    continue
                
                # Check if first version is created by agent
                first_version = versions[0]
                first_evolution = first_version.get("evolution")
                first_change = first_version.get("change")
                first_author = first_version.get("author")
                
                # If clone was created by agent (evolution="None" AND change="None" AND author!="human")
                if (first_evolution == "None" and first_change == "None" and 
                    first_author != "human"):
                    
                    has_agent_clones = True  # Mark that this project has agent-created clones
                    
                    # Now analyze all subsequent versions (updates)
                    for version in versions:
                        evolution = version.get("evolution")
                        change = version.get("change")
                        author = version.get("author")
                        
                        # Skip creation (None values)
                        if evolution == "None" and change == "None":
                            continue
                        
                        # Define author group
                        if author == "human":
                            author_group = "human"
                        else:
                            author_group = "agent"
                        
                        # Count change patterns (only Consistent and Inconsistent, exclude Same)
                        if change in ["Consistent", "Inconsistent"]:
                            agent_clone_modifications[author_group][change] += 1
                        
                        # Count evolution patterns (only Add and Subtract, exclude Same)
                        if evolution in ["Add", "Subtract"]:
                            agent_clone_modifications[author_group][evolution] += 1
            
            # Calculate totals for this project
            human_consistent = agent_clone_modifications["human"]["Consistent"]
            human_inconsistent = agent_clone_modifications["human"]["Inconsistent"]
            human_add = agent_clone_modifications["human"]["Add"]
            human_subtract = agent_clone_modifications["human"]["Subtract"]
            
            agent_consistent = agent_clone_modifications["agent"]["Consistent"]
            agent_inconsistent = agent_clone_modifications["agent"]["Inconsistent"]
            agent_add = agent_clone_modifications["agent"]["Add"]
            agent_subtract = agent_clone_modifications["agent"]["Subtract"]
            
            # Separate data for change and evolution patterns
            change_total = human_consistent + human_inconsistent + agent_consistent + agent_inconsistent
            evolution_total = human_add + human_subtract + agent_add + agent_subtract
            
            # Only add project to results if it has agent-created clones
            if has_agent_clones:
                project_results.append({
                    "Project": project_name,
                    "Human_Consistent": human_consistent,
                    "Human_Inconsistent": human_inconsistent,
                    "Agent_Consistent": agent_consistent,
                    "Agent_Inconsistent": agent_inconsistent,
                    "Total_Change": change_total,
                    "Human_Add": human_add,
                    "Human_Subtract": human_subtract,
                    "Agent_Add": agent_add,
                    "Agent_Subtract": agent_subtract,
                    "Total_Evolution": evolution_total
                })
            
        except ET.ParseError:
            print(f"Warning: Could not parse {filename}. Skipping.")
        except Exception as e:
            print(f"Error processing {filename}: {e}")
    
    df = pd.DataFrame(project_results)
    return df

# --- Execution ---
df_agent_clones = analyze_agent_clones_modifications(results_03_path)

if df_agent_clones is not None and not df_agent_clones.empty:
    # Split into two separate tables
    df_change_patterns = df_agent_clones[["Project", "Human_Consistent", "Human_Inconsistent", 
                                           "Agent_Consistent", "Agent_Inconsistent", "Total_Change"]]
    df_evolution_patterns = df_agent_clones[["Project", "Human_Add", "Human_Subtract", 
                                              "Agent_Add", "Agent_Subtract", "Total_Evolution"]]
    
    # Display Change Patterns
    print("\n" + "="*100)
    print("CHANGE PATTERNS ON AGENT-CREATED CLONES (By Project)")
    print("="*100)
    print(df_change_patterns.to_string(index=False))
    
    # Save Change Patterns
    change_output_path = os.path.join(results_04_path, "agent_clones_change_patterns.csv")
    df_change_patterns.to_csv(change_output_path, index=False)
    print(f"\n✓ Saved: {change_output_path}")
    
    # Display Evolution Patterns
    print("\n" + "="*100)
    print("EVOLUTION PATTERNS ON AGENT-CREATED CLONES (By Project)")
    print("="*100)
    print(df_evolution_patterns.to_string(index=False))
    
    # Save Evolution Patterns
    evolution_output_path = os.path.join(results_04_path, "agent_clones_evolution_patterns.csv")
    df_evolution_patterns.to_csv(evolution_output_path, index=False)
    print(f"\n✓ Saved: {evolution_output_path}")
    
    # Calculate and display summary
    print("\n" + "="*100)
    print("SUMMARY")
    print("="*100)
    print(f"Total projects analyzed: {len(df_agent_clones)}")
    print(f"Total change patterns: {df_change_patterns['Total_Change'].sum()}")
    print(f"Total evolution patterns: {df_evolution_patterns['Total_Evolution'].sum()}")
    print("="*100)
else:
    print("No data found.")
