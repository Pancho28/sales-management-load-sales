import pandas as pd
import logging
from dotenv import load_dotenv
import sys
from datetime import datetime
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
        elif len(sys.argv) == 2:
            formatted_date = datetime.now().strftime("%Y-%m-%d")
        else:
            logger.error('Wrong number of arguments')
            return
        logger.info(f'Start process {formatted_date}')
        db = DBConnection(sys.argv[1])
        conection = db.get_conections()
        cur = conection.cursor()
        logger.info('Query locals')
        cur.execute("""SELECT l.id, u.username 
                    FROM user u 
                    inner join local l on l.userId = u.id 
                    where role = "seller" """)
        locals = cur.fetchall()
        for id, username in locals:
            logger.info(f'Query sales for user {username}')
            cur.execute(f"""select l.name as local, o.id as venta, o.totalDl, o.totalBs, 
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
            dfSales = pd.DataFrame(sales, columns=['local', 'venta', 'totalDl', 'totalBs', 'fechacreacion', 'producto', 'categoria', 'precio', 'cantidad', 'fechaentrega'])
            dfSales.fechacreacion = pd.to_datetime(dfSales.fechacreacion)
            dfSales = dfSales.assign(dia=dfSales.fechacreacion.dt.day_name(locale="es_ES"), hora=dfSales.fechacreacion.dt.strftime("%H"))
            logger.info(f'Total sales {dfSales.shape[0]}')
            logger.info(f'Query payments for user {username}')
            cur.execute(f"""select l.name, o.id as venta, o.totalDl, o.totalBs, po.amount, pt.name, pt.currency, 
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
            dfPayments = pd.DataFrame(payments, columns=['local', 'venta', 'totalDl', 'totalBs', 'cantidad', 'pago', 'moneda', 'fechacreacion'])
            logger.info(f'Total payments {dfPayments.shape[0]}')
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