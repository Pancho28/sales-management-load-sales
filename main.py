import pandas as pd
from loguru import logger
from dotenv import load_dotenv
import sys
from datetime import datetime, timedelta
from config.db import DBConnection
from config.alchemy import AlchemyConnection
from helper.enum import dias_semana

def main():
    load_dotenv()
    try:
        db = None
        motor = None
        if len(sys.argv) == 4:
            formatted_date = datetime.strptime(sys.argv[3], "%Y-%m-%d").strftime("%Y-%m-%d")
            if (formatted_date > datetime.now().strftime("%Y-%m-%d")):
                logger.warning('Cannot reprocess future dates')
                return
            logger.info(f'Starting reprocess {formatted_date}')
        elif len(sys.argv) == 3:
            formatted_date = datetime.now().strftime("%Y-%m-%d")
            logger.info(f'Starting process {formatted_date}')
        else:
            logger.error('Wrong number of arguments')
            return
        close_date = datetime.strptime(formatted_date, "%Y-%m-%d") - timedelta(days=1)
        close_date = close_date.strftime("%Y-%m-%d")
        # Conexiones a base de datos orgien
        db = DBConnection(sys.argv[1])
        locals = db.get_locals(formatted_date)
        if len(locals) == 0:
            logger.warning('No locals to process')
            return
        for id, name, username in locals:
            logger.info(f'Total locals {len(locals)}')
            # Flujo de productos vendidos
            sales = db.get_sales(username, id, formatted_date)
            dfSales = pd.DataFrame(sales, columns=['venta', 'totalDl', 'totalBs', 'fechacreacion', 'producto', 'categoria', 'precio', 'cantidad', 'fechaentrega'])
            dfSales.fechacreacion = pd.to_datetime(dfSales.fechacreacion)
            dfSales = dfSales.assign(fechacierre=pd.to_datetime(close_date))
            dfSales = dfSales.assign(dia=dfSales.fechacierre.dt.day_name().map(dias_semana), hora=dfSales.fechacreacion.dt.strftime("%H"))
            salesPaid = db.get_sales_paid(username,id, formatted_date)
            dfSalesPaid = pd.DataFrame(salesPaid, columns=['venta', 'totalDl', 'totalBs', 'fechacreacion', 'producto', 'categoria', 'precio', 'cantidad', 'fechaentrega'])
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
            dfSales.insert(1, 'local', name)
            logger.info(f'Total sales {dfSales.venta.nunique()}')
            logger.info(f'Total sales records {dfSales.shape[0]}')
            # Flujo de pagos
            payments = db.get_payments(username, id, formatted_date)
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
            else:
                logger.info('No payments paid')
            dfPayments = dfPayments.drop_duplicates(subset=['venta', 'totalDl', 'totalBs', 'cantidad', 'pago', 'moneda'])
            dfPayments.insert(1, 'local', name)
            logger.info(f'Total payments {dfPayments.venta.nunique()}')
            logger.info(f'Total payments records {dfPayments.shape[0]}')
            # Flujo de por pagar
            unpaid = db.get_payments_unpaid(username,id)
            dfUnpaid = pd.DataFrame(unpaid, columns=['venta', 'totalDl', 'totalBs', 'nombre', 'apellido', 'fechacreacion'])
            dfUnpaid.insert(1, 'local', name)
            logger.info(f'Total unpaid {dfUnpaid.venta.nunique()}')
            logger.info(f'Total unpaid records {dfUnpaid.shape[0]}')
            #flujo de empleados
            employees = db.get_for_employee(username, id, formatted_date)
            dfEmployees = pd.DataFrame(employees, columns=['venta', 'totalDl', 'totalBs', 'fechacreacion', 'producto', 'categoria', 'precio', 'cantidad'])
            dfEmployees = dfEmployees.assign(fechacierre=pd.to_datetime(close_date))
            dfEmployees = dfEmployees.drop('fechacreacion', axis=1)
            dfEmployees.insert(1, 'local', name)
            logger.info(f'Total employees {dfEmployees.venta.nunique()}')
            logger.info(f'Total employees records {dfEmployees.shape[0]}')
            # Flujo de guardado en archivos
            if sys.argv[2] == 'local':
                logger.info(f'Saving file for {username}')
                writer = pd.ExcelWriter(f'../locals sales/{username}-{formatted_date}.xlsx')
                dfSales.to_excel(writer, sheet_name='ventas', index=False)
                logger.info('Writing sales')
                dfPayments.to_excel(writer, sheet_name='pagos', index=False)
                logger.info('Writing payments')
                if dfUnpaid.shape[0] > 0:
                    logger.info('Writing unpaid')
                    dfUnpaid.to_excel(writer, sheet_name='por pagar', index=False)
                if dfEmployees.shape[0] > 0:
                    logger.info('Writing employees')
                    dfEmployees.to_excel(writer, sheet_name='empleados', index=False)
                writer.close()
                logger.info(f'File saved {username}-{formatted_date}.xlsx')
            # Flujo de guardado en base de datos (Looker)
            elif sys.argv[2] == 'server':
                # Conexion a base de datos destino
                alchemy = AlchemyConnection(sys.argv[1])
                motor = alchemy.getMotor()
                # Se eliminan los datos de la tabla por_pagar, ya que se carga completa en la ejecucion
                alchemy.truncate_table('por_pagar')
                logger.info(f'Saving data for {username} in looker')
                dfSales.to_sql('ventas', con=motor, if_exists='append', index=False)
                logger.info(f'Sales {dfSales.shape[0]} saved')
                dfPayments.to_sql('pagos', con=motor, if_exists='append', index=False)
                logger.info(f'Payments {dfPayments.shape[0]} saved')
                if dfUnpaid.shape[0] > 0:
                    dfUnpaid.to_sql('por_pagar', con=motor, if_exists='append', index=False)
                    logger.info(f'Unpaid {dfUnpaid.shape[0]} saved')
                if dfEmployees.shape[0] > 0:
                    dfEmployees.to_sql('empleados', con=motor, if_exists='append', index=False)
                    logger.info(f'Employees {dfEmployees.shape[0]} saved')
                logger.info(f'Data saved in looker for {username}')
            logger.info(f'Local {name} processed')
    except Exception as e:
        logger.error(e)
    finally:
        if db:
            db.close_conection()
        if motor:
            motor.dispose()
            logger.info('Close engine')
        logger.info('End process')

if __name__ == '__main__':
    main()