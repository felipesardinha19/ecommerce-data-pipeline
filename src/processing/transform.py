import pandas as pd
from pathlib import Path
from datetime import datetime
from src.utils.logger import get_logger
from src.utils.get_latest_file import get_latest_file

logger = get_logger("transform")
TRUSTED_PATH = Path("data/trusted")

def transform_data():
    try:

        latest_file = get_latest_file(Path("data/raw"), "*.json")
        
        if not latest_file:
            logger.warning(f"Nenhum arquivo encontrado na pasta raw para transformação.")
            return None
        
        logger.info(f"Lendo arquivo: {latest_file}")

        logger.info("Iniciando a transformação dos dados.")
        df = pd.read_json(latest_file)
        logger.info(f"Total de registros a serem transformados: {len(df)}")

        df = df.drop(columns=['tags','dimensions', 'warrantyInformation',
                            'returnPolicy', 'meta', 'images', 'thumbnail',], errors='ignore')
        
        output_file = TRUSTED_PATH / f"products_{datetime.now().strftime('%Y%m%d%H%M%S')}.parquet"

        TRUSTED_PATH.mkdir(parents=True, exist_ok=True)

        df.to_parquet(output_file, index=False)
        logger.info(f"Transformação concluida e dados salvos em {output_file}, no formato parquet.")
        return df
    
    except Exception as e:
        logger.exception("Erro durante a transformação dos dados.")
        return None

if __name__== "__main__":
    transform_data()