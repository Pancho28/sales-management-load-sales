import pandas as pd
from loguru import logger
from typing import List

def transform_unpaid(unpaid: List, local_name: str) -> pd.DataFrame:
    """
    Transforma los datos crudos de cuentas por pagar.
    """
    dfUnpaid = pd.DataFrame(unpaid, columns=['venta', 'totalDl', 'totalBs', 'nombre', 'apellido', 'fechacreacion'])
    dfUnpaid.insert(1, 'local', local_name)
    logger.info(f'Total unpaid {dfUnpaid.venta.nunique()}')
    logger.info(f'Total unpaid records {dfUnpaid.shape[0]}')
    
    return dfUnpaid
