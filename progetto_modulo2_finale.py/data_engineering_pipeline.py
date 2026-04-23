import pandas as pd
import os
import sys
import dask.dataframe as dd
from pyspark.sql import functions as F
from pyspark.sql import SparkSession
import matplotlib.pyplot as plt
import seaborn as sns
from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType, StringType
import time


# ---------------------- PARTE 1: PANDAS (Elaborazione file singoli) ----------------------

cartella = 'progetto_modulo2_finale.py/data_local/json'
files = os.listdir(cartella)

totale_generale = 0

# Itera su tutti i file JSON presenti nella cartella
for file in files:
    percorso_file = os.path.join(cartella, file)

    # Legge il file JSON in formato line-delimited
    df = pd.read_json(percorso_file, lines=True)

    # Calcola la somma della colonna 'amount' per il singolo file
    totale_file = df['amount'].sum()

    print(f'Totale per {file}:{totale_file}')

    # Accumula il totale generale
    totale_generale += totale_file

print(f"\nTotale generale: {totale_generale}")


# ---------------------- PARTE 2: DASK (Elaborazione distribuita) ----------------------

# Legge tutti i file JSON contemporaneamente usando Dask
df = dd.read_json("progetto_modulo2_finale.py/data_local/json/*.jsonl", lines=True)

# Raggruppa per regione e calcola la media degli importi
result = df.groupby("region_id")["amount"].mean()

# Esegue realmente il calcolo (lazy evaluation → compute)
final_result = result.compute()

print(final_result)


# ---------------------- PARTE 3: PYSPARK (Batch processing) ----------------------

# Configura le variabili d’ambiente per PySpark
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
os.environ["HADOOP_HOME"] = r"C:\hadoop"
os.environ["PATH"] = r"C:\hadoop\bin" + os.pathsep + os.environ["PATH"]

# Inizializza la SparkSession
spark = SparkSession.builder \
    .appName('progettoModulo2') \
    .config("spark.jars.packages", "") \
    .config("spark.sql.extensions", "") \
    .config("spark.driver.extraJavaOptions", "-Dfile.encoding=UTF-8") \
    .getOrCreate()

cartella_parquet = "progetto_modulo2_finale.py/data_local/parquet"

# Carica i dataset parquet
products = spark.read.parquet(os.path.join(cartella_parquet, "products.parquet"))
regions = spark.read.parquet(os.path.join(cartella_parquet, "regions.parquet"))

# Identifica tutti i file di transazioni
files = os.listdir(cartella_parquet)
transaction_files = [os.path.join(cartella_parquet, f) for f in files if f.startswith("transactions")]

# Carica i file di transazioni
transactions = spark.read.parquet(*transaction_files)

# Mostra un'anteprima dei dati
transactions.show(5)
products.show(5)
regions.show(5)

# Join tra transazioni e prodotti per ottenere la categoria
transactions_products = transactions.join(
    products,
    on='product_id',
    how='left'
)

# Join con regioni per ottenere il nome della regione
transactions_full = transactions_products.join(
    regions,
    on='region_id',
    how='left'
)

# Selezione delle colonne finali
final_df = transactions_full.select(
    'transaction_id',
    'region_name',
    'category',
    'amount',
    'year'
)

print(final_df.head())


# ---------------------- SALVATAGGIO DATI ----------------------

path = 'progetto_modulo2_finale.py/data_local/processed_sales'

# Salvataggio in formato Parquet partizionato per anno
final_df.write \
    .mode('overwrite') \
    .partitionBy('year') \
    .parquet(path)

print("Dati salvati correttamente partizionati per year.")


# ---------------------- ANALISI: FATTURATO PER CATEGORIA ----------------------

# Aggregazione fatturato totale per categoria
fatturato_categoria = final_df.groupby('category') \
    .agg(F.sum('amount').alias('total_revenue'))

# Conversione a Pandas per visualizzazione
pdf = fatturato_categoria.toPandas()

print(pdf)

# Ordinamento per fatturato decrescente
pdf = pdf.sort_values(by='total_revenue', ascending=False)


# ---------------------- VISUALIZZAZIONE (Matplotlib) ----------------------

plt.figure(figsize=(10, 6))

# Grafico a barre del fatturato per categoria
plt.bar(pdf['category'], pdf['total_revenue'], edgecolor='black')

plt.title('Fatturato per categoria')
plt.xlabel('Categoria')
plt.ylabel('Fatturato Totale')

plt.grid(axis='x', linestyle='--', alpha=0.4)
plt.xticks(rotation=45)
plt.tight_layout()

plt.savefig('fatturato_per_categoria1.png')
plt.show()


# ---------------------- VISUALIZZAZIONE (Seaborn) ----------------------

plt.figure(figsize=(10, 6))

# Grafico più avanzato con Seaborn
sns.barplot(data=pdf, x='category', y='total_revenue', hue='category', palette='Blues_r')

plt.title('Fatturato per Categoria')
plt.xlabel('Categoria')
plt.ylabel('Fatturato Totale')

plt.xticks(rotation=45)
plt.tight_layout()

plt.savefig('fatturato_per_categoria2.png')
plt.show()


# ---------------------- PARTE 4: STREAMING (PYSPARK) ----------------------

# Configurazione ambiente per streaming
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
os.environ["HADOOP_HOME"] = r"C:\hadoop"
os.environ["PATH"] = r"C:\hadoop\bin" + os.pathsep + os.environ["PATH"]

# Creazione nuova SparkSession per streaming
spark = (
    SparkSession.builder
    .appName("RealTimeTransactions")
    .master("local[*]")
    .config("spark.sql.shuffle.partitions", "4")
    .config("spark.ui.showConsoleProgress", "false")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")


# Definizione schema esplicito per i dati in streaming
schema = StructType([
    StructField("transaction_id", StringType(), nullable=True),
    StructField("region_id", StringType(), nullable=True),
    StructField("product_id", StringType(), nullable=True),
    StructField("amount", DoubleType(), nullable=True),
    StructField("year", IntegerType(), nullable=True),
])

# Percorso della cartella monitorata
JSON_PATH = os.path.join(os.path.dirname(__file__), "data_local", "json")

# Verifica esistenza cartella
if not os.path.isdir(JSON_PATH):
    raise FileNotFoundError(f"Cartella non trovata: {JSON_PATH}")


# Lettura dello stream JSON
df_stream = (
    spark.readStream
    .schema(schema)
    .option("cleanSource", "off")
    .option("latestFirst", "false")
    .json(JSON_PATH)
)

# Aggregazione in tempo reale
agg_df = (
    df_stream
    .groupBy("region_id")
    .agg(
        F.count("*").alias("total_transactions"),
        F.round(F.sum("amount"), 2).alias("total_amount")
    )
    .orderBy(F.col("total_transactions").desc())
)


# Funzione custom per stampare ogni micro-batch
def stampa_header(batch_df, batch_id):
    count = batch_df.count()
    print(f"  BATCH #{batch_id}  —  Regioni attive: {count}")

    batch_df.show(truncate=False)


# Avvio dello stream
query = (
    agg_df.writeStream
    .outputMode("complete")
    .foreachBatch(stampa_header)
    .trigger(processingTime="5 seconds")
    .option(
        "checkpointLocation",
        os.path.join(os.path.dirname(__file__), "data_local", "_checkpoint_streaming")
    )
    .start()
)

# Loop per mantenere attivo lo streaming
try:
    print("Stream avviato. In attesa di nuovi file JSON in:", JSON_PATH)
    print('Premere CTRL + C per chiudere la stream.')

    while query.isActive:
        time.sleep(1)

except KeyboardInterrupt:
    print("\nArresto in corso...")

    try:
        query.stop()
        spark.stop()
        print("Stream fermato correttamente.")
    except Exception:
        pass