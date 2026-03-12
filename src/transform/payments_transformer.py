import pandas as pd
from loguru import logger
from typing import List

def transform_payments(payments: List, payments_paid: List, close_date: str, local_name: str) -> pd.DataFrame:
    """
    Transforma y consolida los datos crudos de pagos.
    """
    dfPayments = pd.DataFrame(payments, columns=['venta', 'totalDl', 'totalBs', 'cantidad', 'pago', 'moneda', 'fechacreacion'])
    dfPayments = dfPayments.assign(fechacierre=pd.to_datetime(close_date))
    dfPayments = dfPayments.drop('fechacreacion', axis=1, errors='ignore')
    
    dfPaymentsPaid = pd.DataFrame(payments_paid, columns=['venta', 'totalDl', 'totalBs', 'cantidad', 'pago', 'moneda', 'fechacreacion'])
    dfPaymentsPaid = dfPaymentsPaid.assign(fechacierre=pd.to_datetime(close_date))
    dfPaymentsPaid = dfPaymentsPaid.drop('fechacreacion', axis=1, errors='ignore')
    
    if dfPaymentsPaid.shape[0] > 0:
        logger.info(f'Total payments paid {dfPaymentsPaid.venta.nunique()}')
        if dfPayments.shape[0] == 0:
            dfPayments = dfPaymentsPaid.copy()
        else:
            dfPayments = pd.concat([dfPayments, dfPaymentsPaid], ignore_index=True)
    else:
        logger.info('No payments paid')
        
    dfPayments = dfPayments.drop_duplicates(subset=['venta', 'totalDl', 'totalBs', 'cantidad', 'pago', 'moneda'])
    dfPayments.insert(1, 'local', local_name)
    logger.info(f'Total payments {dfPayments.venta.nunique()}')
    logger.info(f'Total payments records {dfPayments.shape[0]}')
    
    return dfPayments
