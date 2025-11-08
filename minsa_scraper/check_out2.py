import pandas as pd
from pathlib import Path
path = Path('C:/Users/rodri/minsa_scraper/outputs/oferentes_catalogos.xlsx')
df = pd.read_excel(path)
print(df.columns.tolist())
print(df.head())
print('rows', len(df))
