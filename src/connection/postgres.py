import os
from dotenv import load_dotenv
from sqlalchemy import create_engine
from src.utils.logger import get_logger

logger = get_logger("postgres")

load_dotenv()

def get_engine():
    try:
        DB_HOST = os.getenv("DB_HOST")
        DB_PORT = os.getenv("DB_PORT")
        DB_NAME = os.getenv("DB_NAME")
        DB_USER = os.getenv("DB_USER")
        DB_PASSWORD = os.getenv("DB_PASSWORD")
        DB_TYPE = os.getenv("DB_TYPE")

        logger.info("Criando conexão com o banco de dados...")
        
        connection_string = (
            f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}"
            f"@{DB_HOST}:{DB_PORT}/{DB_NAME}"
        )
        engine = create_engine(
            connection_string,
            connect_args={"client_encoding": "utf8"}
        )

        return engine
    
    except Exception as e:
        logger.exception(f"Erro ao criar a conexão com o banco: {e}")
        return None