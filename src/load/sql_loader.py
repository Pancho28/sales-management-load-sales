import pandas as pd
from loguru import logger
from config.alchemy import AlchemyConnection

def load_to_sql(alchemy: AlchemyConnection, motor, username: str, dfSales: pd.DataFrame, dfPayments: pd.DataFrame, dfUnpaid: pd.DataFrame, dfEmployees: pd.DataFrame, check_drop: bool) -> bool:
    """
    Guarda los DataFrames procesados en la base de datos SQL (Looker).
    """
    if check_drop:
        # Se verifica que no se haya borrado la tabla y luego se elimina
        # esto evita que borre la tabla por cada local a cargar
        # el proceso inserta toda la data disponible de cuentas por pagar
        alchemy.truncate_table('por_pagar')
        check_drop = False

    logger.info(f'Saving data for {username} in looker')
    
    if dfSales is not None and dfSales.shape[0] > 0:
        dfSales.to_sql('ventas', con=motor, if_exists='append', index=False)
        logger.info(f'Sales {dfSales.shape[0]} saved')
        
    if dfPayments is not None and dfPayments.shape[0] > 0:
        dfPayments.to_sql('pagos', con=motor, if_exists='append', index=False)
        logger.info(f'Payments {dfPayments.shape[0]} saved')
        
    if dfUnpaid is not None and dfUnpaid.shape[0] > 0:
        dfUnpaid.to_sql('por_pagar', con=motor, if_exists='append', index=False)
        logger.info(f'Unpaid {dfUnpaid.shape[0]} saved')
        
    if dfEmployees is not None and dfEmployees.shape[0] > 0:
        dfEmployees.to_sql('empleados', con=motor, if_exists='append', index=False)
        logger.info(f'Employees {dfEmployees.shape[0]} saved')
        
    logger.info(f'Data saved in looker for {username}')
    return check_drop
