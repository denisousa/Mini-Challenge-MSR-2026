import os
import xml.etree.ElementTree as ET
from utils.folders_paths import results_03_path

print("Starting to fix author='last' to author='human' in XML files...")

# Process all XML files in 03_results
fixed_count = 0
total_changes = 0

for filename in os.listdir(results_03_path):
    if not filename.endswith(".xml"):
        continue
    
    file_path = os.path.join(results_03_path, filename)
    
    try:
        tree = ET.parse(file_path)
        root = tree.getroot()
        
        changes_in_file = 0
        
        # Find all version elements with author="last"
        for version in root.findall(".//version"):
            if version.get("author") == "last":
                version.set("author", "human")
                changes_in_file += 1
        
        # Only write back if there were changes
        if changes_in_file > 0:
            tree.write(file_path, encoding="utf-8", xml_declaration=True)
            fixed_count += 1
            total_changes += changes_in_file
            print(f"✓ {filename}: {changes_in_file} changes")
        
    except ET.ParseError:
        print(f"✗ Error parsing XML: {filename}")
        continue

print(f"\n=== Summary ===")
print(f"Files modified: {fixed_count}")
print(f"Total author attributes changed: {total_changes}")
print("Done!")
