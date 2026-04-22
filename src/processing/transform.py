import pandas as pd
from pathlib import Path
from datetime import datetime
from src.utils.logger import get_logger

logger = get_logger("transform")
TRUSTED_PATH = Path("data/trusted")

def transform_data(input_file):
    try:
                
        logger.info(f"Lendo arquivo: {input_file}")

        logger.info("Iniciando a transformação dos dados.")
        df = pd.read_json(input_file)
        logger.info(f"Total de registros a serem transformados: {len(df)}")

        df = df.drop(columns=['tags','dimensions', 'warrantyInformation',
                            'returnPolicy', 'meta', 'images', 'thumbnail',], errors='ignore')
        
        TRUSTED_PATH.mkdir(parents=True, exist_ok=True)
        
        output_file = TRUSTED_PATH / f"products_{datetime.now().strftime('%Y%m%d%H%M%S')}.parquet"

        df.to_parquet(output_file, index=False)
        logger.info(f"Transformação concluida e dados salvos em {output_file}, no formato parquet.")
        
        return str(output_file)
    
    except Exception as e:
        logger.exception("Erro durante a transformação dos dados.")
        return None