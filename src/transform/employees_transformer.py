import pandas as pd
from loguru import logger
from typing import List

def transform_employees(employees: List, close_date: str, local_name: str) -> pd.DataFrame:
    """
    Transforma los datos crudos de ventas de empleados.
    """
    dfEmployees = pd.DataFrame(employees, columns=['venta', 'totalDl', 'totalBs', 'fechacreacion', 'producto', 'categoria', 'precio', 'cantidad'])
    dfEmployees = dfEmployees.assign(fechacierre=pd.to_datetime(close_date))
    dfEmployees = dfEmployees.drop('fechacreacion', axis=1, errors='ignore')
    dfEmployees.insert(1, 'local', local_name)
    logger.info(f'Total employees {dfEmployees.venta.nunique()}')
    logger.info(f'Total employees records {dfEmployees.shape[0]}')
    
    return dfEmployees
