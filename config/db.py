import os
import pymysql
import logging

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

class DBConnection:

    def __init__(self, enviroment):
        if enviroment == "dev":
            logger.info('Development enviroment')
            self.db_host = os.getenv("DATABASE_HOST_DEV")
            self.db_user = os.getenv("DATABASE_USERNAME_DEV")
            self.db_password = os.getenv("DATABASE_PASSWORD_DEV")
            self.db_name = os.getenv("DATABASE_NAME_DEV")
            self.db_port = int(os.getenv("DATABASE_PORT_DEV"))
        elif enviroment == 'prod':
            logger.info('Production enviroment')
            self.db_host = os.getenv("DATABASE_HOST")
            self.db_user = os.getenv("DATABASE_USERNAME")
            self.db_password = os.getenv("DATABASE_PASSWORD")
            self.db_name = os.getenv("DATABASE_NAME")
            self.db_port = int(os.getenv("DATABASE_PORT"))
        logger.info('Creating conection')
        self.conection = pymysql.connect(host=self.db_host, user=self.db_user, password=self.db_password, db=self.db_name, port=self.db_port)
        self.cur = self.conection.cursor()
    
    def close_conection(self):
        self.conection.close() 
        logger.info('Close conection')

    def get_locals(self, date):
        logger.info('Query locals')
        self.cur.execute(f"""SELECT l.id, l.name, u.username 
                                FROM user u 
                                inner join local l on l.userId = u.id
                                inner join (
                                      select o.localId, date(CONVERT_TZ(o.creationDate,  @@session.time_zone, '-04:00')) as fechacreacion 
                                		from orders o 
                                		group by o.localId, fechacreacion
                                		having fechacreacion >= CONCAT(DATE_ADD('{date}', INTERVAL -1 DAY), ' 11:00:00')
                                		AND fechacreacion <= CONCAT(date('{date}'), ' 11:00:00')
                                ) o on o.localId = l.id
                                where role = "seller"
                                group by l.id, u.username; """)
        locals = self.cur.fetchall()
        return locals
    
    def get_sales(self,username,tiendaId,date):
        logger.info(f'Query sales for user {username}')
        self.cur.execute(f"""select o.id as venta, o.totalDl, o.totalBs, 
                        CONVERT_TZ(o.creationDate,  @@session.time_zone, '-04:00') as fechacreacion, 
                        p.name as producto, c.name as categoria, oi.price, oi.quantity, 
                        CONVERT_TZ(o.deliveredDate,  @@session.time_zone, '-04:00') as fechaentrega
                        from orders o
                        inner join local l on l.id = o.localId
                        inner join order_item oi on oi.orderId = o.id
                        inner join product p on p.id = oi.productId
                        inner join category c on c.id = p.categoryId
                        inner join payment_order po on po.orderId = o.id
                        where l.id = '{tiendaId}'
                        and po.isPaid = 1
                        having fechacreacion >= CONCAT(DATE_ADD('{date}', INTERVAL -1 DAY), ' 11:00:00')
                        AND fechacreacion <= CONCAT(date('{date}'), ' 11:00:00');""")
        sales = self.cur.fetchall()
        return sales
    
    def get_sales_paid(self,username,tiendaId,date):
        logger.info(f'Query sales paid for user {username}')
        self.cur.execute(f"""select o.id as venta, o.totalDl, o.totalBs, 
                                  CONVERT_TZ(ci.paymentDate,  @@session.time_zone, '-04:00') as fechacreacion, 
                                  p.name as producto, c.name as categoria, oi.price, oi.quantity, 
                                  CONVERT_TZ(o.deliveredDate,  @@session.time_zone, '-04:00') as fechaentrega
                            from orders o
                            inner join local l on l.id = o.localId
                            inner join order_item oi on oi.orderId = o.id
                            inner join product p on p.id = oi.productId
                            inner join category c on c.id = p.categoryId
                            inner join payment_order po on o.id = po.orderId
                            inner join customer_information ci on po.id = ci.paymentOrderId
                            where l.id = '{tiendaId}'
                            having fechacreacion >= CONCAT(DATE_ADD('{date}', INTERVAL -1 DAY), ' 11:00:00')
                            AND fechacreacion <= CONCAT(date('{date}'), ' 11:00:00');""")
        sales = self.cur.fetchall()
        return sales
    
    def get_payments(self,username,tiendaId,date):
        logger.info(f'Query payments for user {username}')
        self.cur.execute(f"""select o.id as venta, o.totalDl, o.totalBs, po.amount, pt.name, pt.currency, 
                            CONVERT_TZ(o.creationDate,  @@session.time_zone, '-04:00') as fechacreacion
                            from orders o
                            inner join local l on l.id = o.localId
                            inner join payment_order po on po.orderId = o.id
                            inner join payment_local p on p.id = po.paymentId
                            inner join payment_type pt on pt.id = p.paymentTypeId
                            where l.id = '{tiendaId}' 
                            and po.isPaid = 1
                            having fechacreacion >= CONCAT(DATE_ADD('{date}', INTERVAL -1 DAY), ' 11:00:00')
                            AND fechacreacion <= CONCAT(date('{date}'), ' 11:00:00');""")
        payments = self.cur.fetchall()
        return payments
    
    def get_payments_paid(self,username,tiendaId,date):
        logger.info(f'Query payments paid for user {username}')
        self.cur.execute(f"""select o.id as venta, o.totalDl, o.totalBs, po.amount, pt.name, pt.currency, 
                                   CONVERT_TZ(o.creationDate,  @@session.time_zone, '-04:00') as fechacreacion
                            from orders o
                            inner join local l on l.id = o.localId
                            inner join payment_order po on po.orderId = o.id
                            inner join payment_local p on p.id = po.paymentId
                            inner join payment_type pt on pt.id = p.paymentTypeId
                            inner join (
                            		select po.orderId, CONVERT_TZ(ci.paymentDate,  @@session.time_zone, '-04:00') as fechapago
                                    from payment_order po
                                    inner join customer_information ci on po.id = ci.paymentOrderId
                                    having fechapago >= CONCAT(DATE_ADD('{date}', INTERVAL -1 DAY), ' 11:00:00')
                            		AND fechapago <= CONCAT(date('{date}'), ' 11:00:00')
                                    ) ci on ci.orderId = o.id 
                            where l.id = '{tiendaId}';""")
        payments = self.cur.fetchall()
        return payments
    
    def get_payments_unpaid(self,username,tiendaId):
        logger.info(f'Query payments unpaids for user {username}')
        self.cur.execute(f"""select o.id as venta, o.totalDl, o.totalBs, ci.name, ci.lastName,
                            CONVERT_TZ(o.creationDate,  @@session.time_zone, '-04:00') as fechacreacion
                            from orders o
                            inner join local l on l.id = o.localId
                            inner join payment_order po on po.orderId = o.id
                            inner join payment_local p on p.id = po.paymentId
                            inner join payment_type pt on pt.id = p.paymentTypeId
                            inner join customer_information ci on po.id = ci.paymentOrderId
                            where l.id = '{tiendaId}' 
                            and po.isPaid = 0;""")
        payments = self.cur.fetchall()
        return payments
    
    def get_for_employee(self,username,tiendaId,date):
        logger.info(f'Query for employee for user {username}')
        self.cur.execute(f"""select o.id as venta, o.totalDl, o.totalBs, 
                            CONVERT_TZ(o.creationDate,  @@session.time_zone, '-04:00') as fechacreacion, 
                            p.name as producto, c.name as categoria, oi.price, oi.quantity
                            from orders o
                            inner join local l on l.id = o.localId
                            inner join order_item oi on oi.orderId = o.id
                            inner join product p on p.id = oi.productId
                            inner join category c on c.id = p.categoryId
                            inner join payment_order po on po.orderId = o.id
                            inner join payment_local pl on po.paymentId = pl.id
                            inner join payment_type pt on pt.id = pl.paymentTypeId
                            where l.id = '{tiendaId}'
                            and po.isPaid = 0
                            and pt.name = 'Para empleado'
                            having fechacreacion >= CONCAT(DATE_ADD('{date}', INTERVAL -1 DAY), ' 11:00:00')
                            AND fechacreacion <= CONCAT(date('{date}'), ' 11:00:00');""")
        sales = self.cur.fetchall()
        return sales