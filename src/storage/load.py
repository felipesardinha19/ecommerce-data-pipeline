from sqlalchemy import create_engine
import pandas as pd
from pathlib import Path
from src.utils.logger import get_logger
from src.utils.get_latest_file import get_latest_file
from src.connection.postgres import get_engine
import os

os.environ["PGCLIENTENCODING"] = "UTF8"

logger = get_logger('load')

engine = get_engine()

def get_latest_per_metric():
    files = list(Path("data/analytics").glob("*.parquet"))
    latest_files = {}

    for file in files:
        metric_name = file.name.split("_")[0]  # ajuste se necessário

        if metric_name not in latest_files:
            latest_files[metric_name] = file
        else:
            if file.stat().st_mtime > latest_files[metric_name].stat().st_mtime:
                latest_files[metric_name] = file

    return latest_files.values()

def load():
    try:
        connection = engine
        if connection is None:
            logger.error("Falha ao conectar no banco.")
            return

        files = get_latest_per_metric()

        for file in files:
            df = pd.read_parquet(file)

            if "average_price_category" in file.name:
                table_name = "average_price_category"
            elif "price_min_max_category" in file.name:
                table_name = "price_min_max_category"
            elif "price_discount" in file.name:
                table_name = "price_discount"
            else:
                continue

            df.to_sql(table_name, con=engine, if_exists="replace", index=False)

            logger.info(f"{file} carregado em {table_name}")

    except Exception as e:
        logger.exception(f"Erro ao carregar dados no banco: {e}")
    
if __name__== "__main__":
    load()