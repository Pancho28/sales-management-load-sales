import pymysql
import pandas as pd
import logging
from dotenv import load_dotenv
import os
import datetime

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)
load_dotenv()

def main():
    try:
        today = datetime.datetime.now()
        formatted_date = today.strftime("%Y-%m-%d")
        logger.info(f'Start process {formatted_date}')
        db_host = os.getenv("DATABASE_HOST")
        db_user = os.getenv("DATABASE_USERNAME")
        db_password = os.getenv("DATABASE_PASSWORD")
        db_name = os.getenv("DATABASE_NAME")
        db_port = int(os.getenv("DATABASE_PORT"))
        conection = pymysql.connect(host=db_host, user=db_user, password=db_password, db=db_name, port=db_port)
        cur = conection.cursor()
        logger.info('Query locals')
        cur.execute(""" SELECT l.id, u.username 
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
            dfSales = dfSales.assign(fecha=dfSales.fechacreacion.dt.date, hora=dfSales.fechacreacion.dt.time)
            logger.info(f'Total sales {dfSales.shape[0]}')
            logger.info(f'Query payments for user {username}')
            cur.execute(f"""select l.name, o.id as venta, o.totalDl, o.totalBs, po.amount, pt.name, pt.currency
                            from orders o
                            inner join local l on l.id = o.localId
                            inner join payment_order po on po.orderId = o.id
                            inner join payment_local p on p.id = po.paymentId
                            inner join payment_type pt on pt.id = p.paymentTypeId
                            where l.id = '{id}' 
                            AND o.creationdate >= CONCAT(DATE_ADD('{formatted_date}', INTERVAL -1 DAY), ' 11:00:00')
                            AND o.creationdate <= CONCAT(date('{formatted_date}'), ' 11:00:00')""")
            payments = cur.fetchall()
            dfPayments = pd.DataFrame(payments, columns=['local', 'venta', 'totalDl', 'totalBs', 'amount', 'paymentType', 'currency'])
            logger.info(f'Total payments {dfPayments.shape[0]}')
            writer = pd.ExcelWriter(f'../locals sales/{username}-{formatted_date}.xlsx')
            dfSales.to_excel(writer, sheet_name='ventas')
            dfPayments.to_excel(writer, sheet_name='pagos')
            writer.close()
    except Exception as e:
        logger.error(e)
    finally:
        logger.info('close conection')
        conection.close()

if __name__ == '__main__':
    main()