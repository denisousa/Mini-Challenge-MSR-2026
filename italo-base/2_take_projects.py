import os

metadata_dir = "metadata"
projects = []

for file in os.listdir(metadata_dir):
    if file.endswith(".csv") or file.endswith(".xlsx"):
        project_name = os.path.splitext(file)[0]
        projects.append(project_name)

projects = sorted(set(projects))

print("\nProjetos encontrados:")
for p in projects:
    print(p)

print("\nLinha para colar no settings.ini:")
print("projects = " + ", ".join(projects))
print(len(projects))
