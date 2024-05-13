import pymysql
import pandas as pd
import logging
from dotenv import load_dotenv
import os

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)
load_dotenv()

def main():
    try:
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
            cur.execute(f"""select l.name, o.id as venta, o.totalDl, o.totalBs, date(o.creationdate) as fecha, hour(o.creationdate) as hora,
                            p.name, oi.price, oi.quantity
                            from sales.order o
                            inner join sales.local l on l.id = o.localId
                            inner join sales.order_item oi on oi.orderId = o.id
                            inner join sales.product p on p.id = oi.productId
                            where l.id = '{id}'""")
            sales = cur.fetchall()
            dfSales = pd.DataFrame(sales, columns=['local', 'venta', 'totalDl', 'totalBs', 'fecha', 'hora', 'producto', 'precio', 'cantidad'])
            logger.info(f'Total sales {dfSales.shape[0]}')
            logger.info(f'Query payments for user {username}')
            cur.execute(f"""select l.name, o.id as venta, o.totalDl, o.totalBs, po.amount, pt.name, pt.currency
                            from sales.order o
                            inner join sales.local l on l.id = o.localId
                            inner join sales.payment_order po on po.orderId = o.id
                            inner join sales.payment_type pt on pt.id = po.paymentTypeId
                            where l.id = '{id}' """)
            payments = cur.fetchall()
            dfPayments = pd.DataFrame(payments, columns=['local', 'venta', 'totalDl', 'totalBs', 'amount', 'paymentType', 'currency'])
            logger.info(f'Total payments {dfPayments.shape[0]}')
            writer = pd.ExcelWriter(f'../locals sales/{username}.xlsx')
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