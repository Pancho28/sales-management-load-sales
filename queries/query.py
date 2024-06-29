query_locals = """SELECT l.id, u.username 
                    FROM user u 
                    inner join local l on l.userId = u.id 
                    where role = "%s" """

query_sales = """select l.name as local, o.id as venta, o.totalDl, o.totalBs, 
                            CONVERT_TZ(o.creationDate,  @@session.time_zone, '-04:00') as fechacreacion, 
                            p.name as producto, c.name as categoria, oi.price, oi.quantity, 
                            CONVERT_TZ(o.deliveredDate,  @@session.time_zone, '-04:00') as fechaentrega
                            from orders o
                            inner join local l on l.id = o.localId
                            inner join order_item oi on oi.orderId = o.id
                            inner join product p on p.id = oi.productId
                            inner join category c on c.id = p.categoryId
                            where l.id = '%s'
                            having fechacreacion >= CONCAT(DATE_ADD('%s', INTERVAL -1 DAY), ' 11:00:00')
                            AND fechacreacion <= CONCAT(date('%s'), ' 11:00:00')"""

query_payments = """select l.name, o.id as venta, o.totalDl, o.totalBs, po.amount, pt.name, pt.currency, 
                            CONVERT_TZ(o.creationDate,  @@session.time_zone, '-04:00') as fechacreacion
                            from orders o
                            inner join local l on l.id = o.localId
                            inner join payment_order po on po.orderId = o.id
                            inner join payment_local p on p.id = po.paymentId
                            inner join payment_type pt on pt.id = p.paymentTypeId
                            where l.id = '%s' 
                            having fechacreacion >= CONCAT(DATE_ADD('%s', INTERVAL -1 DAY), ' 11:00:00')
                            AND fechacreacion <= CONCAT(date('%s'), ' 11:00:00')"""


