import pandas as pd
from loguru import logger
from typing import List
from helper.enum import dias_semana

def transform_sales(sales: List, sales_paid: List, close_date: str, local_name: str) -> pd.DataFrame:
    """
    Transforma y consolida los datos crudos de ventas.
    """
    dfSales = pd.DataFrame(sales, columns=['venta', 'totalDl', 'totalBs', 'fechacreacion', 'producto', 'categoria', 'precio', 'cantidad', 'fechaentrega'])
    dfSales.fechacreacion = pd.to_datetime(dfSales.fechacreacion)
    dfSales = dfSales.assign(fechacierre=pd.to_datetime(close_date))
    dfSales = dfSales.assign(dia=dfSales.fechacierre.dt.day_name().map(dias_semana), hora=dfSales.fechacreacion.dt.strftime("%H"))
    
    dfSalesPaid = pd.DataFrame(sales_paid, columns=['venta', 'totalDl', 'totalBs', 'fechacreacion', 'producto', 'categoria', 'precio', 'cantidad', 'fechaentrega'])
    dfSalesPaid.fechacreacion = pd.to_datetime(dfSalesPaid.fechacreacion)
    dfSalesPaid = dfSalesPaid.assign(fechacierre=pd.to_datetime(close_date))
    dfSalesPaid = dfSalesPaid.assign(dia=dfSalesPaid.fechacierre.dt.day_name().map(dias_semana), hora=dfSalesPaid.fechacreacion.dt.strftime("%H"))
    
    if dfSalesPaid.shape[0] > 0:
        logger.info(f'Total sales paid {dfSalesPaid.venta.nunique()}')
        if dfSales.shape[0] == 0:
            dfSales = dfSalesPaid.copy()
        else:
            dfSales = pd.concat([dfSales, dfSalesPaid], ignore_index=True)
    else:
        logger.info('No sales paid')
        
    dfSales = dfSales.drop_duplicates(subset=['venta', 'totalDl', 'totalBs', 'producto', 'categoria', 'precio', 'cantidad'])
    dfSales.insert(1, 'local', local_name)
    logger.info(f'Total sales {dfSales.venta.nunique()}')
    logger.info(f'Total sales records {dfSales.shape[0]}')
    
    return dfSales
