with open('visitors.txt', 'r') as file:
    log_entries = file.readlines()

# Extracting the names using regular expression pattern
import re

names = []
for entry in log_entries:
    match = re.search(r'Client DATA:\s*(.*?),\s*Balance', entry)
    if match:
        names.append(match.group(1))

# Output the extracted names
for name in names:
    print(name)