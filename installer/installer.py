import os
import json

user_input = input("Do you want to install SEDB?(y/n):")

# install sedb
if user_input == 'y' or user_input == 'Y':
    with open('version.json', 'r') as f:
        version = json.load(f)
    
    print("Install SEDB v{version[version]} {version[Bata or Stable]}\n")