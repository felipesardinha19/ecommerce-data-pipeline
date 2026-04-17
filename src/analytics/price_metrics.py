import pandas as pd
import numpy as np
from pathlib import Path
from src.utils.logger import get_logger
from src.utils.get_latest_file import get_latest_file

logger = get_logger("metrics")
ANALYTICS_PATH = Path("data/analytics")

def load_latest_parquet_file():
    latest_file = get_latest_file(Path("data/trusted"), "*.parquet")
    if not latest_file:
        logger.warning("Nenhum arquivo encontrado.")
        return None
    return latest_file

def average_price_category(file):
    try:
        df = pd.read_parquet(file)

        #média de preço por categoria
        df['average_price_category'] = df.groupby('category')['price'].transform('mean').round(2)
        logger.info("Média de preço por categoria calculada com sucesso.")

        #formatando df
        logger.info("Iniciando formatação da tabela.")
        df = df[['id', 'title', 'category', 'stock', 'price', 'average_price_category']].fillna(np.nan)
        output_file = ANALYTICS_PATH / f"average_price_category_{pd.Timestamp.now().strftime('%y-%m-%d_%H-%M-%S')}.parquet"

        ANALYTICS_PATH.mkdir(parents=True, exist_ok=True)
        df.to_parquet(output_file, index=False)
        logger.info(f"Métricas salvas em {output_file}.")

        return df
    
    except Exception as e:
        logger.exception("Erro ao calcular a média de preço por categoria: {e}.")
        return None
    
def price_min_max_category(file):
    try:
        df = pd.read_parquet(file)

        #preço minimo e maximo por categoria
        df['price_min_category'] = df.groupby('category')['price'].transform('min').round(2)
        df['price_max_category'] = df.groupby('category')['price'].transform('max').round(2)

        logger.info("Preço minimo e maximo por categoria calculado com sucesso.")

        #fromatando df
        df = df[['id', 'title', 'category', 'stock', 'price', 'price_min_category', 'price_max_category']].fillna(np.nan)
        output_file = ANALYTICS_PATH / f"price_min_max_category_{pd.Timestamp.now().strftime('%y-%m-%d_%H-%M-%S')}.parquet"
        
        ANALYTICS_PATH.mkdir(parents=True, exist_ok=True)
        df.to_parquet(output_file, index=False)
        logger.info(f"Métricas salvas em {output_file}")
        return df

    except Exception as e:
        logger.exception("Erro ao calcular o preço mínimo e máximo por categoria.")
        return None
    
def price_discount_percentage(file):
    try:
        df = pd.read_parquet(file)

        df['discount_value'] = (df['price'] * (df['discountPercentage'] / 100)).round(2)
        df['final_price'] = (df['price'] * ( 1 - df['discountPercentage'] / 100)).round(2)

        logger.info("Descontos calculados com sucesso!")

        #formatando df
        df = df[['id', 'title', 'category', 'stock', 'brand', 'price', 'discount_value', 'final_price']]
        output_file = ANALYTICS_PATH / f"price_discount{pd.Timestamp.now().strftime('%y-%m-%d_%H-%M-%S')}.parquet"

        ANALYTICS_PATH.mkdir(parents=True, exist_ok=True)
        df.to_parquet(output_file, index=False)
        logger.info(f"Métricas salvas em {output_file}")

        return df

    except Exception as e:
        logger.exception("Erro ao calcular preço com desconto")
        return None

def run_analytics():
    file = load_latest_parquet_file()

    if file is None:
        logger.warning("Nenhum arquivo para processar no analytics.")
        return

    average_price_category(file)
    price_min_max_category(file)
    price_discount_percentage(file)

    logger.info("Pipeline de analytics executado com sucesso.")