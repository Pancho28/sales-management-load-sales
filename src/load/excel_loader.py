import pandas as pd
from loguru import logger
import os

def load_to_excel(username: str, formatted_date: str, dfSales: pd.DataFrame, dfPayments: pd.DataFrame, dfUnpaid: pd.DataFrame, dfEmployees: pd.DataFrame):
    """
    Guarda los DataFrames procesados en un archivo Excel.
    """
    logger.info(f'Saving file for {username}')
    
    # Asegurar que el directorio existe
    os.makedirs('../locals sales', exist_ok=True)
    
    file_path = f'../locals sales/{username}-{formatted_date}.xlsx'
    writer = pd.ExcelWriter(file_path)
    
    dfSales.to_excel(writer, sheet_name='ventas', index=False)
    logger.info('Writing sales')
    
    dfPayments.to_excel(writer, sheet_name='pagos', index=False)
    logger.info('Writing payments')
    
    if dfUnpaid is not None and dfUnpaid.shape[0] > 0:
        logger.info('Writing unpaid')
        dfUnpaid.to_excel(writer, sheet_name='por pagar', index=False)
        
    if dfEmployees is not None and dfEmployees.shape[0] > 0:
        logger.info('Writing employees')
        dfEmployees.to_excel(writer, sheet_name='empleados', index=False)
        
    writer.close()
    logger.info(f'File saved {username}-{formatted_date}.xlsx')
