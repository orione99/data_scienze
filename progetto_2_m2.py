import pandas as pd
import numpy as np

# Creazione del primo file csv da 100.000 righe

num_ordini = 100_000
num_clienti = 5000
num_prodotti = 20

start = pd.Timestamp('2025-01-01')
end = pd.Timestamp('2025-12-31')
range_giorni = (end - start).days


ordini = pd.DataFrame({
    'clienti_ID' : np.random.randint(1,num_clienti, size= num_ordini),
    'prodotto_ID' : np.random.randint(1,num_prodotti, size=num_ordini),
    'quantita' : np.random.randint(1,100,size=num_ordini),
    'data' : start + pd.to_timedelta(np.random.randint(0, range_giorni + 1, size = num_ordini), unit='D')
})

ordini.to_csv('Ordini.csv', index=False)
print('Primo file CSV :\n', ordini)

# Creazione del secondo file, in questo caso JSON

categorie = ['Elettronica', 'Sport', 'Casa', 'Giochi', 'Abbigliamento']
fornitori = ['Fornitore_1', 'Fornitore_2', 'Fornitore_3', 'Fornitore_4']

prodotti = pd.DataFrame({
    'prodotto_ID' : np.arange(1, num_prodotti + 1),
    'categoria' : [np.random.choice(categorie) for _ in range(num_prodotti)],
    'fornitore' : [np.random.choice(fornitori) for _ in range(num_prodotti)]
})

prodotti.to_json('prodotti.json', orient='records')
print('secondo file :\n', prodotti)

# Creazione del terzo file, un altro CSV

regioni = ['Nord', 'Centro', 'Sud', 'Isole']
segmenti = ['A', 'B', 'C', 'D']

clienti = pd.DataFrame({
    'clienti_ID' : np.arange(1,num_clienti +1),
    'regione' : [np.random.choice(regioni) for _ in range(num_clienti)],
    'segmento' : [np.random.choice(segmenti) for _ in range(num_clienti)]
})

clienti.to_csv('clienti.csv', index=False)
print('Terzo file :\n', clienti)

# Unisco i tre file per raccogliere tutte le informazione in un solo Dataset

df1 = pd.merge(prodotti,ordini, on='prodotto_ID') 
df_finale = pd.merge(df1, clienti, on='clienti_ID')

df_finale = df_finale.sort_values('data', ascending=True)  # ordiniamolo in ordine crescente per data, cosi da avere un quadro migliore dell'anno
print(df_finale)

# Verico la memoria attualmente utilizzata
print(df_finale.memory_usage(deep=True))
print(f'Memoria totale occupata: {(df_finale.memory_usage(deep=True).sum()) / 1024**2:.2f} MB')

# Ottimizzo la memoria

df_finale['prodotto_ID'] = pd.to_numeric(df_finale['prodotto_ID'], downcast='integer')
df_finale['categoria'] = df_finale['categoria'].astype('category')
df_finale['fornitore'] = df_finale['fornitore'].astype('category')
df_finale['clienti_ID'] = pd.to_numeric(df_finale['clienti_ID'], downcast='integer')
df_finale['quantita'] = pd.to_numeric(df_finale['quantita'], downcast='integer')
df_finale['regione'] = df_finale['regione'].astype('category')
df_finale['segmento'] = df_finale['segmento'].astype('category')


#Verifico la memoria utilizzata adesso

print(df_finale.memory_usage(deep=True))
print(f'Memoria totale occupata: {(df_finale.memory_usage(deep=True).sum()) / 1024**2:.2f} MB')

prezzi = np.random.randint(10, 50, size=num_prodotti)
prezzi_dict = dict(zip(prodotti['prodotto_ID'], prezzi))
df_finale['prezzo'] = df_finale['prodotto_ID'].map(prezzi_dict)
df_finale['valore_totale'] = pd.to_numeric(df_finale['prezzo'] * df_finale['quantita'], downcast='integer')
print(df_finale)

filtro_ordini = df_finale.query("valore_totale > 100 and clienti_ID")
print(filtro_ordini)