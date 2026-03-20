import pandas as pd
import numpy as np

# Seed di riproducibilità
np.random.seed(42)

# Numero righe Dataset
n = 100000

# Date casuali
date_range = pd.date_range(start='2025-01-01', end='2025-12-31')
order_dates = np.random.choice(date_range, size= n)
ship_dates = order_dates + pd.to_timedelta(np.random.randint(1,5, size = n), unit='D')

# Categorie e Sottocategorie
# Sottocategorie definite
sub_categories = ['Chairs', 'Binders', 'Phones', 'Tables', 'Copiers']
subcategory_to_category = {
    'Chairs': 'Furniture',
    'Tables': 'Furniture',
    'Binders': 'Office Supplies',
    'Phones': 'Technology',
    'Copiers': 'Technology'
}
sub_category = np.random.choice(sub_categories, size=n)

# Vendite e profitti casuali
sales = np.random.uniform(10,1000,size=n)
profit = sales * np.random.uniform(0.3,0.5,size=n)

# Regioni e zone

regions = {
    'Nord' : ['Lombardia','Piemonte'],
    'Centro' : ['Toscana','Lazio'],
    'Sud' : ['Campania','Puglia']
    }
areas = list(regions.keys())
area = np.random.choice(areas, size = n)
region = [np.random.choice(regions[a]) for a in area]

# Quantita vendute
quantity = np.random.randint(1,20, size=n)


df = pd.DataFrame({
    'order_dates' : order_dates,
    'ship_dates' : ship_dates,
    'sub_category' : sub_category,
    'category' : pd.Series(sub_category).map(subcategory_to_category),
    'sales' : sales.round(2),
    'profit' : profit.round(2),
    'area' : area,
    'regions' : region,
    'quantity' : quantity   
})


# Mostro le prime righe
print(df.sort_values('order_dates').head())

# Ottimizziamo la memoria
df['order_dates'] = pd.to_datetime(df['order_dates'])
df['ship_dates'] = pd.to_datetime(df['ship_dates'])
df['sub_category'] = df['sub_category'].astype('category')
df['category'] = df['category'].astype('category')
df['sales'] = df['sales'].astype('float')
df['profit'] = df['profit'].astype('float')
df['area'] = df['area'].astype('category')

# Stampo l'utilizzo di memoria
print(f'Memoria usata: {df.memory_usage(deep=True).sum() / (1024**2)} MB')

# Calcolo profitto mensile e quantita venduta mensile
profit_mens = df.groupby(pd.Grouper(key='order_dates', freq='ME'))['profit'].sum()
quantity_mens = df.groupby(pd.Grouper(key='order_dates', freq='ME'))['quantity'].sum()

print(f'Profitti mensili:\n',profit_mens)
print(f'Quantità vendute mensili:\n',quantity_mens)

# Sub-category piu vendute 
sub_cat_piu_vednute = df.groupby('sub_category')['quantity'].sum().sort_values(ascending=False)
print(f'Top 5 sub category per quantita vednute:\n',sub_cat_piu_vednute)

# Creo le coordinate per le singole regioni
region_coords = {
    'Lombardia': (45.4642, 9.1900),
    'Piemonte': (45.0703, 7.6869),
    'Toscana': (43.7711, 11.2486),
    'Lazio': (41.9028, 12.4964),
    'Campania': (40.8518, 14.2681),
    'Puglia': (41.1256, 16.8667)
}

# Creo le due nuove colonne latitudine e longitudine
df['lat'] = df['regions'].map(lambda x: region_coords[x][0])
df['lon'] = df['regions'].map(lambda x: region_coords[x][1])

map_df = df.groupby(['regions', 'lat', 'lon'], as_index=False).agg({
    'sales': 'sum',
    'profit': 'sum',
    'quantity': 'sum'
})

# Importo plotly.express per creare una mappa topografica interattiva che rappresenti le vendite
import plotly.express as px

fig = px.scatter_mapbox(
    map_df,
    lat="lat",
    lon="lon",
    size="sales",
    color="profit",
    hover_name="regions",
    hover_data={
        "sales": True,
        "profit": True,
        "quantity": True
    },
    color_continuous_scale="Viridis",
    size_max=50,
    zoom=5,
    title="Vendite per Regione"
)

fig.update_layout(mapbox_style="open-street-map")

# Mostro il risultato
fig.show()