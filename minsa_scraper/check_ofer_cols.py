import pandas as pd
from pathlib import Path
path = Path('C:/Users/rodri/minsa_scraper/outputs/oferentes_catalogos.xlsx')
if path.exists():
    df = pd.read_excel(path)
    cols = [c for c in df.columns if 'Oferente' in c]
    print(cols)
