import pandas as pd
import numpy as np
from pathlib import Path
from src.utils.logger import get_logger
from datetime import datetime

logger = get_logger("metrics")
ANALYTICS_PATH = Path("data/analytics")

def average_price_category(df):
    try:
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
    
def price_min_max_category(df):
    try:
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
        logger.exception(f"Erro ao calcular o preço mínimo e máximo por categoria: {e}.")
        return None
    
def price_discount_percentage(df):
    try:
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
        logger.exception(f"Erro ao calcular preço com desconto: {e}")
        return None

def save_metric(df, name):
    if df is None:
        logger.warning(f"{name} não gerado.")
        return None
    
    ANALYTICS_PATH.mkdir(parents=True, exist_ok=True)

    output_file =  ANALYTICS_PATH / f"{name}_{datetime.now().strftime('%y-%m-%d_%H-%M-%S')}.parquet"
    df.to_parquet(output_file, index=False)

    logger.info(f"{name} salvo em {output_file}")

    return str(output_file)

def run_analytics(file_path):
    try:
        if not file_path:
            logger.warning("Nenhum arquivo recebido para analytics.")
            return None
        
        logger.info(f"Rodando analytics para: {file_path}")

        df = pd.read_parquet(file_path)

        avg_df = average_price_category(df.copy())
        minmax_df = price_min_max_category(df.copy())
        discount_df = price_discount_percentage(df.copy())

        avg_path = save_metric(avg_df, "average_price_category")
        minmax_path = save_metric(minmax_df, "price_min_max_category")
        discount_path = save_metric(discount_df, "price_discount")

        logger.info("Analytics executado com sucesso.")

        return {
            "average_price_category": avg_path,
            "price_min_max_category": minmax_path,
            "price_discount": discount_path
        }

    except Exception as e:
        logger.exception(f"Erro ao processar analises: {e}")