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
            # Flujo de productos vendidos
            sales = db.get_sales(username,id, formatted_date)
            dfSales = pd.DataFrame(sales, columns=['venta', 'totalDl', 'totalBs', 'fechacreacion', 'producto', 'categoria', 'precio', 'cantidad', 'fechaentrega'])
            dfSales.fechacreacion = pd.to_datetime(dfSales.fechacreacion)
            dfSales = dfSales.assign(fechacierre=pd.to_datetime(close_date))
            dfSales = dfSales.assign(dia=dfSales.fechacierre.dt.day_name(locale="es_ES"), hora=dfSales.fechacreacion.dt.strftime("%H"))
            salesPaid = db.get_sales_paid(username,id, formatted_date)
            dfSalesPaid = pd.DataFrame(salesPaid, columns=['venta', 'totalDl', 'totalBs', 'fechacreacion', 'producto', 'categoria', 'precio', 'cantidad', 'fechaentrega'])
            dfSalesPaid.fechacreacion = pd.to_datetime(dfSalesPaid.fechacreacion)
            dfSalesPaid = dfSalesPaid.assign(fechacierre=pd.to_datetime(close_date))
            dfSalesPaid = dfSalesPaid.assign(dia=dfSalesPaid.fechacierre.dt.day_name(locale="es_ES"), hora=dfSalesPaid.fechacreacion.dt.strftime("%H"))
            if dfSalesPaid.shape[0] > 0:
                logger.info(f'Total sales paid {dfSalesPaid.venta.nunique()}')
                if dfSales.shape[0] == 0:
                    dfSales = dfSalesPaid.copy()
                else:
                    dfSales = pd.concat([dfSales, dfSalesPaid], ignore_index=True)
            dfSales = dfSales.drop_duplicates(subset=['venta', 'totalDl', 'totalBs', 'producto', 'categoria', 'precio', 'cantidad'])
            logger.info(f'Total sales {dfSales.venta.nunique()}')
            logger.info(f'Total sales records {dfSales.shape[0]}')
            # Flujo de pagos
            payments = db.get_payments(username,id, formatted_date)
            dfPayments = pd.DataFrame(payments, columns=['venta', 'totalDl', 'totalBs', 'cantidad', 'pago', 'moneda', 'fechacreacion'])
            dfPayments = dfPayments.assign(fechacierre=pd.to_datetime(close_date))
            dfPayments = dfPayments.drop('fechacreacion', axis=1)
            paymentsPaid = db.get_payments_paid(username,id, formatted_date)
            dfPaymentsPaid = pd.DataFrame(paymentsPaid, columns=['venta', 'totalDl', 'totalBs', 'cantidad', 'pago', 'moneda', 'fechacreacion'])
            dfPaymentsPaid = dfPaymentsPaid.assign(fechacierre=pd.to_datetime(close_date))
            dfPaymentsPaid = dfPaymentsPaid.drop('fechacreacion', axis=1)
            if dfPaymentsPaid.shape[0] > 0:
                logger.info(f'Total payments paid {dfPaymentsPaid.venta.nunique()}')
                if dfPayments.shape[0] == 0:
                    dfPayments = dfPaymentsPaid.copy()
                else:
                    dfPayments = pd.concat([dfPayments, dfPaymentsPaid], ignore_index=True)
            dfPayments = dfPayments.drop_duplicates(subset=['venta', 'totalDl', 'totalBs', 'cantidad', 'pago', 'moneda'])
            logger.info(f'Total payments {dfPayments.venta.nunique()}')
            logger.info(f'Total payments records {dfPayments.shape[0]}')
            # Flujo de por pagar
            unpaid = db.get_payments_unpaid(username,id)
            dfUnpaid = pd.DataFrame(unpaid, columns=['venta', 'totalDl', 'totalBs', 'nombre', 'apellido', 'fechacreacion'])
            logger.info(f'Total unpaid {dfUnpaid.venta.nunique()}')
            logger.info(f'Total unpaid records {dfUnpaid.shape[0]}')
            # Flujo de guardado
            writer = pd.ExcelWriter(f'../locals sales/{username}-{formatted_date}.xlsx')
            dfSales.to_excel(writer, sheet_name='ventas', index=False)
            dfPayments.to_excel(writer, sheet_name='pagos', index=False)
            if dfUnpaid.shape[0] > 0:
                logger.info(f'Saving unpaid')
                dfUnpaid.to_excel(writer, sheet_name='por pagar', index=False)
            writer.close()
    except Exception as e:
        logger.error(e)
    finally:
        if db:
            db.close_conection()
        logger.info('End process')

if __name__ == '__main__':
    main()