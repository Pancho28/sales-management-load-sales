import sys
from datetime import datetime, timedelta
from loguru import logger
from dataclasses import dataclass

@dataclass
class AppConfig:
    env: str
    destination: str
    formatted_date: str
    close_date: str

def parse_arguments() -> AppConfig:
    """
    Parsea los argumentos de la línea de comandos para determinar la fecha 
    de procesamiento y el destino ('local' o 'server').
    """
    if len(sys.argv) == 4:
        formatted_date = datetime.strptime(sys.argv[3], "%Y-%m-%d").strftime("%Y-%m-%d")
        if (formatted_date > datetime.now().strftime("%Y-%m-%d")):
            logger.warning('Cannot reprocess future dates')
            sys.exit(1)
        logger.info(f'Starting reprocess {formatted_date}')
    elif len(sys.argv) == 3:
        formatted_date = datetime.now().strftime("%Y-%m-%d")
        logger.info(f'Starting process {formatted_date}')
    else:
        logger.error('Wrong number of arguments')
        sys.exit(1)
        
    env = sys.argv[1]
    destination = sys.argv[2]

    # Fecha de cierre calculada al día restando 1 a la de procesamiento
    close_date_obj = datetime.strptime(formatted_date, "%Y-%m-%d") - timedelta(days=1)
    close_date = close_date_obj.strftime("%Y-%m-%d")

    return AppConfig(
        env=env,
        destination=destination,
        formatted_date=formatted_date,
        close_date=close_date
    )
