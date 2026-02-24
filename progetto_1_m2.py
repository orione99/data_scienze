import pandas as pd
import numpy as np


date = pd.date_range(start='2025-01-01', end='2025-12-31', freq='d')
prodotti = np.random.choice(['prodotto A', 'prodotto B', 'prodotto C'], size=len(date)) 
vendite = np.random.randint(0,1000,len(date))
prezzo = {
    'prodotto A': 2.5,
    'prodotto B': 6.0,
    'prodotto C': 8.0
}
df = pd.DataFrame({
    'date' : date,
    'prodotto' : prodotti,
    'vendite' : vendite
})
df['prezzo'] = df['prodotto'].map(prezzo)



print(df.head())
print(df.info())
print(df.describe())

df.loc[df.index[:15],'vendite'] = np.nan

df['vendite'] = df['vendite'].fillna(df['vendite'].mean()).round(2)

df = df.drop_duplicates(subset=['date','prodotto'])
df['date'] = pd.to_datetime(df['date'])


print(df)

vendite_x_prodotto = df.groupby('prodotto')['vendite'].sum()
print(vendite_x_prodotto)

pro_piu_vend = vendite_x_prodotto.idxmax()
pro_meno_vend = vendite_x_prodotto.idxmin()

vendite_medi_gio = df.groupby('date')['vendite'].mean()

print(f'Vendite per prodotto: {vendite_x_prodotto} \nProdotto piu venduto: {pro_piu_vend} \nProdotto meno venduto: {pro_meno_vend}')
print(f'Vendite medie giornaliere: {vendite_medi_gio}')



