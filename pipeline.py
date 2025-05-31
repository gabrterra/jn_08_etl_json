from etl import pipeline

pasta: str = 'data'
formato: list = ["parquet", "csv"]

pipeline(pasta, formato)