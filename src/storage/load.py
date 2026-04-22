import pandas as pd
from pathlib import Path
from src.utils.logger import get_logger
from src.connection.postgres import get_engine
import os

os.environ["PGCLIENTENCODING"] = "UTF8"

logger = get_logger('load')

def load_data(files_dict):
    try:
        if not files_dict:
            logger.warning("Nenhum arquivo recebido para carga.")
            return
        
        engine = get_engine()

        if engine is None:
            logger.error("Falha ao conectar no banco.")
            return

        for table_name, file_path in files_dict.items():
            logger.info(f"Carregando {file_path} na tabela {table_name}")

            df = pd.read_parquet(file_path)

            df.to_sql(table_name,
                    con=engine,
                    if_exists="append",
                    index=False)

            logger.info(f"Tabela {table_name}, carregada com sucesso.")

        logger.info("Carga finalizada com sucesso.")

    except Exception as e:
        logger.exception(f"Erro ao carregar dados no banco: {e}")
    
if __name__== "__main__":
    load_data()