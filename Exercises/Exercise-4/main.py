import json
import glob
import csv
from pathlib import Path
import pandas as pd

def main():
    # your code here
    data = Path('data')
    for path in data.rglob('*.json'):
        if path.is_file():
            with open(path, 'r', encoding='utf-8') as file:
                data = json.load(file)
                df = pd.json_normalize(data)
                df.to_csv(str(path).replace('json', 'csv'), index=False)




if __name__ == "__main__":
    main()
