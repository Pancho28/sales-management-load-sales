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
        db_host = os.getenv("DATABASE_HOST")
        db_user = os.getenv("DATABASE_USERNAME")
        db_password = os.getenv("DATABASE_PASSWORD")
        db_name = os.getenv("DATABASE_NAME")
        conection = pymysql.connect(host=db_host, user=db_user, password=db_password, db=db_name)
        cur = conection.cursor()
        logger.info('Query locals')
        cur.execute(""" SELECT l.id, u.username 
                    FROM user u 
                    inner join local l on l.userId = u.id 
                    where role = "seller" """)
        locals = cur.fetchall()
        for id, username in locals:
            logger.info(f'Query sales for user {username}')
            cur.execute(f"""select l.name as local, o.id as venta, o.totalDl, o.totalBs, date(o.creationdate) as fecha, hour(o.creationdate) as hora,
                            p.name as producto, c.name as categoria, oi.price, oi.quantity
                            from sales.order o
                            inner join sales.local l on l.id = o.localId
                            inner join sales.order_item oi on oi.orderId = o.id
                            inner join sales.product p on p.id = oi.productId
                            inner join sales.category c on c.id = p.categoryId
                            where l.id = '{id}'
                            AND o.creationdate >= CONCAT(DATE_ADD('{formatted_date}', INTERVAL -1 DAY), ' 11:00:00')
                            AND o.creationdate <= CONCAT(date('{formatted_date}'), ' 11:00:00')""")
            sales = cur.fetchall()
            dfSales = pd.DataFrame(sales, columns=['local', 'venta', 'totalDl', 'totalBs', 'fecha', 'hora', 'producto', 'categoria', 'precio', 'cantidad'])
            logger.info(f'Total sales {dfSales.shape[0]}')
            logger.info(f'Query payments for user {username}')
            cur.execute(f"""select l.name, o.id as venta, o.totalDl, o.totalBs, po.amount, pt.name, pt.currency
                            from sales.order o
                            inner join sales.local l on l.id = o.localId
                            inner join sales.payment_order po on po.orderId = o.id
                            inner join sales.payment_type pt on pt.id = po.paymentTypeId
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