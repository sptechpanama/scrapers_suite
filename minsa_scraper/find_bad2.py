import pandas as pd
from pathlib import Path
path = Path('C:/Users/rodri/minsa_scraper/outputs/oferentes_catalogos.xlsx')
df = pd.read_excel(path)
first = df.columns[df.columns.str.contains('Cat', na=False)][0]
bad = df[df[first].astype(str).str.strip().str.match(r'^[\d\.]+$', na=False)]
print('bad rows', len(bad))
