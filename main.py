import pandas as pd
import logging
from dotenv import load_dotenv
import sys
from datetime import datetime, timedelta
from config.db import DBConnection

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)
load_dotenv()

def main():
    try:
        db = None
        if len(sys.argv) == 3:
            formatted_date = datetime.strptime(sys.argv[2], "%Y-%m-%d").strftime("%Y-%m-%d")
            logger.info(f'Starting reprocess {formatted_date}')
        elif len(sys.argv) == 2:
            formatted_date = datetime.now().strftime("%Y-%m-%d")
            logger.info(f'Starting process {formatted_date}')
        else:
            logger.error('Wrong number of arguments')
            return
        close_date = datetime.strptime(formatted_date, "%Y-%m-%d") - timedelta(days=1)
        close_date = close_date.strftime("%Y-%m-%d")
        db = DBConnection(sys.argv[1])
        locals = db.get_locals(formatted_date)
        if len(locals) == 0:
            logger.warning('No locals to process')
            return
        logger.info(f'Total locals {len(locals)}')
        for id, username in locals:
            sales = db.get_sales(username,id, formatted_date)
            dfSales = pd.DataFrame(sales, columns=['venta', 'totalDl', 'totalBs', 'fechacreacion', 'producto', 'categoria', 'precio', 'cantidad', 'fechaentrega'])
            dfSales.fechacreacion = pd.to_datetime(dfSales.fechacreacion)
            dfSales = dfSales.assign(fechacierre=pd.to_datetime(close_date))
            dfSales = dfSales.assign(dia=dfSales.fechacierre.dt.day_name(locale="es_ES"), hora=dfSales.fechacreacion.dt.strftime("%H"))
            logger.info(f'Total sales {dfSales.venta.nunique()}')
            logger.info(f'Total sales records {dfSales.shape[0]}')
            payments = db.get_payments(username,id, formatted_date)
            dfPayments = pd.DataFrame(payments, columns=['venta', 'totalDl', 'totalBs', 'cantidad', 'pago', 'moneda', 'fechacreacion'])
            dfPayments = dfPayments.assign(fechacierre=pd.to_datetime(close_date))
            dfPayments = dfPayments.drop('fechacreacion', axis=1)
            logger.info(f'Total payments {dfPayments.venta.nunique()}')
            logger.info(f'Total payments records {dfPayments.shape[0]}')
            writer = pd.ExcelWriter(f'../locals sales/{username}-{formatted_date}.xlsx')
            dfSales.to_excel(writer, sheet_name='ventas', index=False)
            dfPayments.to_excel(writer, sheet_name='pagos', index=False)
            writer.close()
    except Exception as e:
        logger.error(e)
    finally:
        if db:
            db.close_conection()
        logger.info('End process')

if __name__ == '__main__':
    main()