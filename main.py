import pandas as pd
import logging
from dotenv import load_dotenv
import sys
from datetime import datetime, timedelta
from config.db import DBConnection
from queries.query import query_locals, query_sales, query_payments

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)
load_dotenv()

def main():
    try:
        conection = None
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
        conection = db.get_conections()
        cur = conection.cursor()
        logger.info('Query locals')
        cur.execute(f"""SELECT l.id, u.username 
                    FROM user u 
                    inner join local l on l.userId = u.id
                    inner join (
                    	select o.localId, CONVERT_TZ(o.creationDate,  @@session.time_zone, '-04:00') as fechacreacion 
                    	from orders o 
						having fechacreacion >= CONCAT(DATE_ADD('{formatted_date}', INTERVAL -1 DAY), ' 11:00:00')
                        AND fechacreacion <= CONCAT(date('{formatted_date}'), ' 11:00:00')
						limit 1
                    ) o on o.localId = l.id 
                    where role = "seller" """)
        locals = cur.fetchall()
        if len(locals) == 0:
            logger.warning('No locals to process')
            return
        logger.info(f'Total locals {len(locals)}')
        for id, username in locals:
            logger.info(f'Query sales for user {username}')
            cur.execute(f"""select o.id as venta, o.totalDl, o.totalBs, 
                            CONVERT_TZ(o.creationDate,  @@session.time_zone, '-04:00') as fechacreacion, 
                            p.name as producto, c.name as categoria, oi.price, oi.quantity, 
                            CONVERT_TZ(o.deliveredDate,  @@session.time_zone, '-04:00') as fechaentrega
                            from orders o
                            inner join local l on l.id = o.localId
                            inner join order_item oi on oi.orderId = o.id
                            inner join product p on p.id = oi.productId
                            inner join category c on c.id = p.categoryId
                            where l.id = '{id}'
                            having fechacreacion >= CONCAT(DATE_ADD('{formatted_date}', INTERVAL -1 DAY), ' 11:00:00')
                            AND fechacreacion <= CONCAT(date('{formatted_date}'), ' 11:00:00')""")
            sales = cur.fetchall()
            dfSales = pd.DataFrame(sales, columns=['venta', 'totalDl', 'totalBs', 'fechacreacion', 'producto', 'categoria', 'precio', 'cantidad', 'fechaentrega'])
            dfSales.fechacreacion = pd.to_datetime(dfSales.fechacreacion)
            dfSales = dfSales.assign(fechacierre=pd.to_datetime(close_date))
            dfSales = dfSales.assign(dia=dfSales.fechacierre.dt.day_name(locale="es_ES"), hora=dfSales.fechacreacion.dt.strftime("%H"))
            logger.info(f'Total sales {dfSales.venta.nunique()}')
            logger.info(f'Total sales records {dfSales.shape[0]}')
            logger.info(f'Query payments for user {username}')
            cur.execute(f"""select o.id as venta, o.totalDl, o.totalBs, po.amount, pt.name, pt.currency, 
                            CONVERT_TZ(o.creationDate,  @@session.time_zone, '-04:00') as fechacreacion
                            from orders o
                            inner join local l on l.id = o.localId
                            inner join payment_order po on po.orderId = o.id
                            inner join payment_local p on p.id = po.paymentId
                            inner join payment_type pt on pt.id = p.paymentTypeId
                            where l.id = '{id}' 
                            having fechacreacion >= CONCAT(DATE_ADD('{formatted_date}', INTERVAL -1 DAY), ' 11:00:00')
                            AND fechacreacion <= CONCAT(date('{formatted_date}'), ' 11:00:00')""")
            payments = cur.fetchall()
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
        if conection:
            logger.info('close conection')
            conection.close()
        logger.info('End process')

if __name__ == '__main__':
    main()