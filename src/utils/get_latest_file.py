from pathlib import Path
from src.utils.logger import get_logger

logger = get_logger("get_latest_file")

def get_latest_file(path: Path, pattern: str):
    try:
        pasta = Path(path) 
        if not pasta.exists():
            logger.warning(f"O caminho {path} não existe.")
            return None
         
        arquivos = list(pasta.glob(pattern))

        if not arquivos:
            logger.warning(f"Nenhum arquivo encontrado em {path} com o padrão {pattern}.")
            return None
        
        latest_data = max(arquivos, key=lambda f: f.stat().st_mtime)

        logger.info(f"Arquivo mais recente encontrado: {latest_data}")    
        return latest_data
    
    except Exception as e:
        logger.error(f"Erro ao obter o arquivo mais recente: {e}")
        return None
    
if __name__== "__main__":
    get_latest_file(Path("data/trusted"), "*.parquet")