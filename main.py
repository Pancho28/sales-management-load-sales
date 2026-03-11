"""
Módulo principal para la extracción, transformación y carga (ETL) de datos de ventas.

Este script se encarga de extraer la información de ventas, pagos, cuentas por pagar
y empleados desde una base de datos de origen (MySQL), transformar los 
datos utilizando la librería `pandas`, y cargarlos en archivos Excel locales o en
una base de datos de destino (para análisis en Looker).

Uso vía línea de comandos:
    python main.py <entorno_db> <destino> [fecha_reproceso]
    - <entorno_db>: Entorno de base de datos a utilizar (ej. 'prod', 'dev').
    - <destino>: Destino de los datos ('local' para Excel, 'server' para DB).
    - [fecha_reproceso]: Opcional. Fecha específica a reprocesar en formato YYYY-MM-DD.
"""
import pandas as pd
from loguru import logger
from dotenv import load_dotenv
import sys
from datetime import datetime, timedelta
from config.db import DBConnection
from config.alchemy import AlchemyConnection
from helper.enum import dias_semana
from helper.email_sender import send_email

# Elimina el handler por defecto que envía todo a stderr (Todos los niveles de logs salen como error en railway)
logger.remove()

# Configura un handler para los logs de nivel INFO y DEBUG
# Se envía a stdout y se filtra para que no incluya los logs de nivel WARNING o superior.
logger.add(sys.stdout,
           level="INFO",
           # Usa logger.level para acceder al número de nivel
           filter=lambda record: record["level"].no < logger.level("WARNING").no)

# Configura un segundo handler para los logs de nivel WARNING, ERROR y CRITICAL
# Se envía a stderr.
logger.add(sys.stderr,
           level="WARNING")

def main():
    """
    Función principal que ejecuta el pipeline ETL completo.
    
    Flujo de trabajo:
    1. Carga variables de entorno.
    2. Parsea los argumentos de la línea de comandos para determinar la fecha 
       de procesamiento y el destino ('local' o 'server').
    3. Conecta a la base de datos de origen para obtener la lista de locales activos.
    4. Por cada local iterado:
       - Extrae las ventas realizadas y pagadas.
       - Extrae los pagos realizados.
       - Extrae las cuentas por pagar.
       - Extrae las ventas asociadas a los empleados.
       - Realiza el cruce y limpieza de estas métricas iterando con DataFrames de pandas.
    5. Carga los DataFrames finalmente procesados:
       - En archivos Excel separados por hoja si el modo es 'local'.
       - En una base de datos destino si el modo es 'server'.
    6. Cierra de forma segura las conexiones a bases de datos.
    """
    load_dotenv()
    try:
        db = None
        motor = None
        # Determinación de fecha para el procesamiento a partir de argumentos CLI
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
            
        # Fecha de cierre calculada al día restando 1 a la de procesamiento
        close_date = datetime.strptime(formatted_date, "%Y-%m-%d") - timedelta(days=1)
        close_date = close_date.strftime("%Y-%m-%d")
        
        # Conexiones a base de datos origen
        db = DBConnection(sys.argv[1])
        locals = db.get_locals(formatted_date)
        if len(locals) == 0:
            logger.warning('No locals to process')
            send_email(
                subject=f"Sales Management - Warning: Sin locales a procesar",
                body=f"La ejecución del ETL para la fecha {formatted_date} no retornó locales activos para procesar."
            )
            return
        logger.info(f'Total locals {len(locals)}')
        
        # Conexion a base de datos destino
        alchemy = AlchemyConnection(sys.argv[1])
        motor = alchemy.getMotor()
        check_drop = True
        
        # Procesa de forma independiente la información de cada local
        for id, name, username in locals:
            # ----------------------------------------------------
            # 1. Flujo de productos vendidos
            # ----------------------------------------------------
            sales = db.get_sales(username, id, formatted_date)
            dfSales = pd.DataFrame(sales, columns=['venta', 'totalDl', 'totalBs', 'fechacreacion', 'producto', 'categoria', 'precio', 'cantidad', 'fechaentrega'])
            dfSales.fechacreacion = pd.to_datetime(dfSales.fechacreacion)
            dfSales = dfSales.assign(fechacierre=pd.to_datetime(close_date))
            dfSales = dfSales.assign(dia=dfSales.fechacierre.dt.day_name().map(dias_semana), hora=dfSales.fechacreacion.dt.strftime("%H"))
            
            # Obtiene también los productos que fueron pagados
            salesPaid = db.get_sales_paid(username,id, formatted_date)
            dfSalesPaid = pd.DataFrame(salesPaid, columns=['venta', 'totalDl', 'totalBs', 'fechacreacion', 'producto', 'categoria', 'precio', 'cantidad', 'fechaentrega'])
            dfSalesPaid.fechacreacion = pd.to_datetime(dfSalesPaid.fechacreacion)
            dfSalesPaid = dfSalesPaid.assign(fechacierre=pd.to_datetime(close_date))
            dfSalesPaid = dfSalesPaid.assign(dia=dfSalesPaid.fechacierre.dt.day_name().map(dias_semana), hora=dfSalesPaid.fechacreacion.dt.strftime("%H"))
            
            # Consolida ambas fuentes de ventas
            if dfSalesPaid.shape[0] > 0:
                logger.info(f'Total sales paid {dfSalesPaid.venta.nunique()}')
                if dfSales.shape[0] == 0:
                    dfSales = dfSalesPaid.copy()
                else:
                    dfSales = pd.concat([dfSales, dfSalesPaid], ignore_index=True)
            else:
                logger.info('No sales paid')
                
            # Elimina registros duplicados basándose en coincidencia de métricas clave 
            dfSales = dfSales.drop_duplicates(subset=['venta', 'totalDl', 'totalBs', 'producto', 'categoria', 'precio', 'cantidad'])
            dfSales.insert(1, 'local', name)
            logger.info(f'Total sales {dfSales.venta.nunique()}')
            logger.info(f'Total sales records {dfSales.shape[0]}')
            
            # ----------------------------------------------------
            # 2. Flujo de pagos
            # ----------------------------------------------------
            payments = db.get_payments(username, id, formatted_date)
            dfPayments = pd.DataFrame(payments, columns=['venta', 'totalDl', 'totalBs', 'cantidad', 'pago', 'moneda', 'fechacreacion'])
            dfPayments = dfPayments.assign(fechacierre=pd.to_datetime(close_date))
            dfPayments = dfPayments.drop('fechacreacion', axis=1)
            
            paymentsPaid = db.get_payments_paid(username,id, formatted_date)
            dfPaymentsPaid = pd.DataFrame(paymentsPaid, columns=['venta', 'totalDl', 'totalBs', 'cantidad', 'pago', 'moneda', 'fechacreacion'])
            dfPaymentsPaid = dfPaymentsPaid.assign(fechacierre=pd.to_datetime(close_date))
            dfPaymentsPaid = dfPaymentsPaid.drop('fechacreacion', axis=1)
            
            # Consolida la información cruzada de pagos
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
            
            # ----------------------------------------------------
            # 3. Flujo de cuentas por pagar (unpaid)
            # ----------------------------------------------------
            unpaid = db.get_payments_unpaid(username,id)
            dfUnpaid = pd.DataFrame(unpaid, columns=['venta', 'totalDl', 'totalBs', 'nombre', 'apellido', 'fechacreacion'])
            dfUnpaid.insert(1, 'local', name)
            logger.info(f'Total unpaid {dfUnpaid.venta.nunique()}')
            logger.info(f'Total unpaid records {dfUnpaid.shape[0]}')
            
            # ----------------------------------------------------
            # 4. Flujo de empleados 
            # ----------------------------------------------------
            employees = db.get_for_employee(username, id, formatted_date)
            dfEmployees = pd.DataFrame(employees, columns=['venta', 'totalDl', 'totalBs', 'fechacreacion', 'producto', 'categoria', 'precio', 'cantidad'])
            dfEmployees = dfEmployees.assign(fechacierre=pd.to_datetime(close_date))
            dfEmployees = dfEmployees.drop('fechacreacion', axis=1)
            dfEmployees.insert(1, 'local', name)
            logger.info(f'Total employees {dfEmployees.venta.nunique()}')
            logger.info(f'Total employees records {dfEmployees.shape[0]}')
            
            # ----------------------------------------------------
            # 5. Flujo de guardado 
            # ----------------------------------------------------
            
            # Guarda en archivos Excel (entorno local)
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
                
            # Guarda en la base de datos centralizada (servidor)
            elif sys.argv[2] == 'server':
                if check_drop:
                    # Se verifica que no se haya borrado la tabla y luego se elimina
                    # esto evita que borre la tabla por cada local a cargar
                    # el proceso inserta toda la data disponible de cuentas por pagar
                    alchemy.truncate_table('por_pagar')
                    check_drop = False
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
                
                # ----------------------------------------------------
                # Validación de cuadre de Ventas vs Pagos
                # ----------------------------------------------------
                sales_count = dfSales.venta.nunique() if 'venta' in dfSales.columns else 0
                payments_count = dfPayments.venta.nunique() if 'venta' in dfPayments.columns else 0
                
                if sales_count == payments_count:
                    logger.info(f'Validación OK: Ventas ({sales_count}) == Pagos ({payments_count})')
                    # Se prepara mensaje de éxito
                    total_usd = dfPayments['cantidad'].sum() if 'cantidad' in dfPayments.columns else 0
                    send_email(
                        subject=f"Sales Management - Éxito: {name}",
                        body=f"El ETL para el local '{name}' procesó y se guardó exitosamente.\nLos datos están cuadrados: {sales_count} ventas registradas.\nTotal procesado: ${total_usd:,.2f}"
                    )
                else:
                    logger.warning(f'Descuadre en {name}: Ventas ({sales_count}) != Pagos ({payments_count})')
                    # Se prepara mensaje de error/descuadre
                    send_email(
                        subject=f"Sales Management - Descuadre de datos: {name}",
                        body=f"Se detectó un descuadre en la información para el local '{name}'.\n\nMétricas guardadas:\n- Total Ventas Únicas procesadas: {sales_count}\n- Total Pagos Únicos procesados: {payments_count}\n\nFavor revisar la información origen del local."
                    )
                
            logger.info(f'Local {name} processed')
            
    except Exception as e:
        logger.error(e)
        send_email(
            subject="Sales Management - Error de Ejecución",
            body=f"Ocurrió una excepción durante la ejecución del proceso ETL:\n\n{str(e)}"
        )
    finally:
        # Cerrar conexiones correctamente liberando los recursos de las DB
        if db:
            db.close_conection()
        if motor:
            motor.dispose()
            logger.info('Close engine')
        logger.info('End process')

if __name__ == '__main__':
    main()