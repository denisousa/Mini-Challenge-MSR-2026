import os
import xml.etree.ElementTree as ET
import pandas as pd
from pathlib import Path

def extract_patterns_from_xml(xml_file):
    """
    Extract evolution and change patterns from an XML file
    """
    tree = ET.parse(xml_file)
    root = tree.getroot()
    
    results = {
        'evolution_subtract': [],
        'evolution_add': [],
        'change_inconsistent': [],
        'change_consistent': []
    }
    
    # Iterate over all versions in all lineages
    for lineage in root.findall('.//lineage'):
        for version in lineage.findall('version'):
            evolution = version.get('evolution', '')
            change = version.get('change', '')
            n_evo = int(version.get('n_evo', '0'))
            n_cha = int(version.get('n_cha', '0'))
            author = version.get('author', '')
            number_pr = version.get('number_pr', '')
            
            # Evolution Patterns
            if evolution == 'Subtract':
                results['evolution_subtract'].append({
                    'n_evo': n_evo,
                    'author': author,
                    'pr': number_pr
                })
            elif evolution == 'Add':
                results['evolution_add'].append({
                    'n_evo': n_evo,
                    'author': author,
                    'pr': number_pr
                })
            
            # Change Patterns
            if change == 'Inconsistent':
                results['change_inconsistent'].append({
                    'n_cha': n_cha,
                    'author': author,
                    'pr': number_pr
                })
            elif change == 'Consistent':
                results['change_consistent'].append({
                    'n_cha': n_cha,
                    'author': author,
                    'pr': number_pr
                })
    
    return results

def process_all_xml_files(directory):
    """
    Process all XML files in a directory
    """
    all_evolution_data = []
    all_change_data = []
    
    xml_files = list(Path(directory).glob('*.xml'))
    print(f"Processing {len(xml_files)} XML files from {directory}...")
    
    for xml_file in xml_files:
        print(f"  Processing {xml_file.name}...")
        
        # Extract information from filename
        filename = xml_file.stem  # Remove .xml extension
        parts = filename.split('_')
        
        if len(parts) >= 3:
            language = parts[0]
            repo_owner = parts[1]
            repo_name = '_'.join(parts[2:])
        else:
            language = 'unknown'
            repo_owner = 'unknown'
            repo_name = filename
        
        results = extract_patterns_from_xml(xml_file)
        
        # Evolution Patterns
        for pattern_type, items in [('Subtract', results['evolution_subtract']), 
                                     ('Add', results['evolution_add'])]:
            total_n_evo = sum(item['n_evo'] for item in items)
            count_occurrences = len(items)
            
            # Separate by author
            human_items = [item for item in items if item['author'] == 'human']
            agent_items = [item for item in items if item['author'] == 'agent']
            
            all_evolution_data.append({
                'project': f"{repo_owner}/{repo_name}",
                'evolution_pattern': pattern_type,
                'total_occurrences': count_occurrences,
                'sum_n_evo': total_n_evo,
                'human_occurrences': len(human_items),
                'human_sum_n_evo': sum(item['n_evo'] for item in human_items),
                'agent_occurrences': len(agent_items),
                'agent_sum_n_evo': sum(item['n_evo'] for item in agent_items)
            })
        
        # Change Patterns
        for pattern_type, items in [('Inconsistent', results['change_inconsistent']), 
                                     ('Consistent', results['change_consistent'])]:
            total_n_cha = sum(item['n_cha'] for item in items)
            count_occurrences = len(items)
            
            # Separate by author
            human_items = [item for item in items if item['author'] == 'human']
            agent_items = [item for item in items if item['author'] == 'agent']
            
            all_change_data.append({
                'project': f"{repo_owner}/{repo_name}",
                'change_pattern': pattern_type,
                'total_occurrences': count_occurrences,
                'sum_n_cha': total_n_cha,
                'human_occurrences': len(human_items),
                'human_sum_n_cha': sum(item['n_cha'] for item in human_items),
                'agent_occurrences': len(agent_items),
                'agent_sum_n_cha': sum(item['n_cha'] for item in agent_items)
            })
    
    return all_evolution_data, all_change_data

def main():
    # Process XML files from 03_results folder
    base_dir = '/home/denis/Mini-Challenge-MSR-2026'
    xml_dir = os.path.join(base_dir, '03_results')
    
    evolution_data, change_data = process_all_xml_files(xml_dir)
    
    # Create DataFrames
    df_evolution = pd.DataFrame(evolution_data)
    df_change = pd.DataFrame(change_data)
    
    # Calculate averages
    df_evolution['avg_n_evo_per_occurrence'] = df_evolution['sum_n_evo'] / df_evolution['total_occurrences'].replace(0, 1)
    df_evolution['human_avg_n_evo'] = df_evolution['human_sum_n_evo'] / df_evolution['human_occurrences'].replace(0, 1)
    df_evolution['agent_avg_n_evo'] = df_evolution['agent_sum_n_evo'] / df_evolution['agent_occurrences'].replace(0, 1)
    
    df_change['avg_n_cha_per_occurrence'] = df_change['sum_n_cha'] / df_change['total_occurrences'].replace(0, 1)
    df_change['human_avg_n_cha'] = df_change['human_sum_n_cha'] / df_change['human_occurrences'].replace(0, 1)
    df_change['agent_avg_n_cha'] = df_change['agent_sum_n_cha'] / df_change['agent_occurrences'].replace(0, 1)
    
    # Add summary statistics
    print("\n" + "="*80)
    print("EVOLUTION PATTERNS SUMMARY")
    print("="*80)
    print(df_evolution.groupby('evolution_pattern').agg({
        'total_occurrences': 'sum',
        'sum_n_evo': 'sum',
        'human_occurrences': 'sum',
        'human_sum_n_evo': 'sum',
        'agent_occurrences': 'sum',
        'agent_sum_n_evo': 'sum'
    }))
    
    print("\n" + "="*80)
    print("CHANGE PATTERNS SUMMARY")
    print("="*80)
    print(df_change.groupby('change_pattern').agg({
        'total_occurrences': 'sum',
        'sum_n_cha': 'sum',
        'human_occurrences': 'sum',
        'human_sum_n_cha': 'sum',
        'agent_occurrences': 'sum',
        'agent_sum_n_cha': 'sum'
    }))
    
    # Save the CSVs
    output_dir = os.path.join(base_dir, '11_results')
    os.makedirs(output_dir, exist_ok=True)
    
    evolution_csv = os.path.join(output_dir, 'evolution_patterns_summary.csv')
    change_csv = os.path.join(output_dir, 'change_patterns_summary.csv')
    summary_txt = os.path.join(output_dir, 'patterns_summary.txt')
    
    df_evolution.to_csv(evolution_csv, index=False)
    df_change.to_csv(change_csv, index=False)
    
    # Save organized TXT summary
    with open(summary_txt, 'w') as f:
        f.write("="*100 + "\n")
        f.write("EVOLUTION AND CHANGE PATTERNS ANALYSIS SUMMARY\n")
        f.write("="*100 + "\n\n")
        
        # Evolution Patterns - By Project
        f.write("="*100 + "\n")
        f.write("EVOLUTION PATTERNS BY PROJECT\n")
        f.write("="*100 + "\n")
        f.write(df_evolution.to_string(index=False))
        f.write("\n\n")
        
        # Evolution Patterns - Summary
        f.write("="*100 + "\n")
        f.write("EVOLUTION PATTERNS SUMMARY\n")
        f.write("="*100 + "\n")
        evolution_summary = df_evolution.groupby('evolution_pattern').agg({
            'total_occurrences': 'sum',
            'sum_n_evo': 'sum',
            'human_occurrences': 'sum',
            'human_sum_n_evo': 'sum',
            'agent_occurrences': 'sum',
            'agent_sum_n_evo': 'sum'
        })
        f.write(evolution_summary.to_string())
        f.write("\n\n")
        
        # Evolution Patterns - Average Summary
        f.write("="*100 + "\n")
        f.write("EVOLUTION PATTERNS - AVERAGE N_EVO SUMMARY\n")
        f.write("="*100 + "\n")
        evolution_avg_summary = df_evolution.groupby('evolution_pattern').apply(
            lambda x: pd.Series({
                'overall_avg_n_evo': x['sum_n_evo'].sum() / x['total_occurrences'].sum() if x['total_occurrences'].sum() > 0 else 0,
                'human_avg_n_evo': x['human_sum_n_evo'].sum() / x['human_occurrences'].sum() if x['human_occurrences'].sum() > 0 else 0,
                'agent_avg_n_evo': x['agent_sum_n_evo'].sum() / x['agent_occurrences'].sum() if x['agent_occurrences'].sum() > 0 else 0
            })
        ).round(2)
        f.write(evolution_avg_summary.to_string())
        f.write("\n\n")
        
        # Change Patterns - By Project
        f.write("="*100 + "\n")
        f.write("CHANGE PATTERNS BY PROJECT\n")
        f.write("="*100 + "\n")
        f.write(df_change.to_string(index=False))
        f.write("\n\n")
        
        # Change Patterns - Summary
        f.write("="*100 + "\n")
        f.write("CHANGE PATTERNS SUMMARY\n")
        f.write("="*100 + "\n")
        change_summary = df_change.groupby('change_pattern').agg({
            'total_occurrences': 'sum',
            'sum_n_cha': 'sum',
            'human_occurrences': 'sum',
            'human_sum_n_cha': 'sum',
            'agent_occurrences': 'sum',
            'agent_sum_n_cha': 'sum'
        })
        f.write(change_summary.to_string())
        f.write("\n\n")
        
        # Change Patterns - Average Summary
        f.write("="*100 + "\n")
        f.write("CHANGE PATTERNS - AVERAGE N_CHA SUMMARY\n")
        f.write("="*100 + "\n")
        change_avg_summary = df_change.groupby('change_pattern').apply(
            lambda x: pd.Series({
                'overall_avg_n_cha': x['sum_n_cha'].sum() / x['total_occurrences'].sum() if x['total_occurrences'].sum() > 0 else 0,
                'human_avg_n_cha': x['human_sum_n_cha'].sum() / x['human_occurrences'].sum() if x['human_occurrences'].sum() > 0 else 0,
                'agent_avg_n_cha': x['agent_sum_n_cha'].sum() / x['agent_occurrences'].sum() if x['agent_occurrences'].sum() > 0 else 0
            })
        ).round(2)
        f.write(change_avg_summary.to_string())
        f.write("\n")
    
    print(f"\n✅ Files saved:")
    print(f"   - {evolution_csv}")
    print(f"   - {change_csv}")
    print(f"   - {summary_txt}")

if __name__ == '__main__':
    main()
