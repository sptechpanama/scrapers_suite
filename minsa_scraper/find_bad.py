import pandas as pd
from pathlib import Path
path = Path('C:/Users/rodri/minsa_scraper/outputs/oferentes_catalogos.xlsx')
if path.exists():
    df = pd.read_excel(path)
    bad = df[df['Catlogo::Nombre del Producto'].astype(str).str.match(r"^(?:nan|\d+|\.{3})$", case=False)]
    print('bad rows', len(bad))
    print(bad.head())
