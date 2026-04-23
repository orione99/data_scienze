#  Data Engineering Pipeline: Pandas, Dask e PySpark

Prima di avviare il codice presente nel file 'data_engineering_pipeline.py', avviare il file 'generator.py' per i dati necessari.

## Overview

Questo progetto implementa una pipeline completa di data engineering che simula un flusso dati reale utilizzando tre livelli di processing:

- **Pandas** per analisi locale su file JSON
- **Dask** per elaborazione parallela su dataset multipli
- **PySpark** per processing distribuito batch e streaming in tempo reale

L’obiettivo è confrontare diverse tecnologie di data processing e costruire una pipeline completa: dall’ingestione dei dati fino all’analisi e alla visualizzazione finale.

---

## Obiettivi del progetto

- Analizzare dati transazionali da file JSON e Parquet
- Confrontare Pandas, Dask e Spark in termini di approccio e scalabilità
- Implementare pipeline batch con PySpark
- Simulare uno stream di dati in tempo reale
- Generare insight tramite aggregazioni e analisi
- Visualizzare i risultati con grafici

---

## Tecnologie utilizzate

- Python 3.11.5
- Pandas
- Dask
- PySpark (Structured Streaming)
- Matplotlib
- Seaborn
- Hadoop (per ambiente Spark locale)
