import pandas as pd
import os
import glob

# Função que lê e consolida os json
def extrair_dados(pasta: str) -> pd.DataFrame:
    """Extrai dados de todos arquivos .json em uma pasta e retorna um DataFrame concatenado."""
    arquivos_json = glob.glob(os.path.join(pasta, '*.json'))
    df_list = [pd.read_json(arquivo) for arquivo in arquivos_json]
    df = pd.concat(df_list, ignore_index=True)
    return df

# Função que transforma
def calcular_kpi_total_vendas(df: pd.DataFrame) -> pd.DataFrame:
    """Transforma o DataFrame original incluindo uma coluna "Total" que representa o valor total de cada produto em cada linha."""
    df["Total"] = df["Quantidade"] * df["Venda"]
    return df

# Função que dá load em csv ou parquet
def salvar_dados(df: pd.DataFrame, formato_arquivo: list):
    
    for formato in formato_arquivo:
        if formato == 'csv':
            df.to_csv('df.csv', index=False)
        if formato == 'parquet':
            df.to_parquet('df.parquet', index=False)

def pipeline(pasta: str, formato_arquivo: list):
    """
    Executa o pipeline ETL completo, recebendo como parâmetro a pasta contendo os arquivos .json e uma lista de formatos de saída.

    Parameters:
    pasta (str): Caminho da pasta contendo arquivos .json
    formato_arquivo (list): Lista de formatos de saída, podendo ser 'parquet' ou 'csv'

    Returns:
    None
    """
    df = extrair_dados(pasta)
    df = calcular_kpi_total_vendas(df)
    salvar_dados(df, formato_arquivo)





if __name__ == '__main__':
    pasta = 'data'
    formato_arquivo = ["parquet", "csv"]
    pipeline(pasta, formato_arquivo)