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
from loguru import logger
from dotenv import load_dotenv

# Core
from src.core.config import parse_arguments
from src.core.log_setup import setup_logger

# BD & Helpers
from config.db import DBConnection
from config.alchemy import AlchemyConnection
from helper.email_sender import send_email

# Extract
from src.extract.sales_extractor import extract_sales
from src.extract.payments_extractor import extract_payments
from src.extract.unpaid_extractor import extract_unpaid
from src.extract.employees_extractor import extract_employees

# Transform
from src.transform.sales_transformer import transform_sales
from src.transform.payments_transformer import transform_payments
from src.transform.unpaid_transformer import transform_unpaid
from src.transform.employees_transformer import transform_employees

# Load
from src.load.excel_loader import load_to_excel
from src.load.sql_loader import load_to_sql

# Utils
from src.utils.validators import validate_sales_vs_payments, validate_locals_list

def main():
    """
    Función principal que ejecuta el pipeline ETL completo.
    """
    load_dotenv()
    
    # 1. Configuración e Inicialización
    setup_logger()
    config = parse_arguments()
    
    db = None
    motor = None
    
    try:
        # 2. Conexiones a Base de Datos
        db = DBConnection(config.env)
        locals_list = db.get_locals(config.formatted_date)
        
        if not validate_locals_list(locals_list, config.formatted_date):
            return
        
        alchemy = AlchemyConnection(config.env)
        motor = alchemy.getMotor()
        check_drop = True
        
        # 3. Procesamiento Independiente por Local
        for id, name, username in locals_list:
            
            # --- EXTRACT ---
            raw_sales, raw_paid_sales = extract_sales(db, username, id, config.formatted_date)
            raw_payments, raw_paid_payments = extract_payments(db, username, id, config.formatted_date)
            raw_unpaid = extract_unpaid(db, username, id)
            raw_employees = extract_employees(db, username, id, config.formatted_date)
            
            # --- TRANSFORM ---
            df_sales = transform_sales(raw_sales, raw_paid_sales, config.close_date, name)
            df_payments = transform_payments(raw_payments, raw_paid_payments, config.close_date, name)
            
            df_unpaid = None
            if len(raw_unpaid) > 0:
                df_unpaid = transform_unpaid(raw_unpaid, name)
                
            df_employees = None
            if len(raw_employees) > 0:
                df_employees = transform_employees(raw_employees, config.close_date, name)
            
            # --- LOAD ---
            if config.destination == 'local':
                load_to_excel(username, config.formatted_date, df_sales, df_payments, df_unpaid, df_employees)
                
            elif config.destination == 'server':
                check_drop = load_to_sql(
                    alchemy, motor, username, 
                    df_sales, df_payments, df_unpaid, df_employees, 
                    check_drop
                )
                
                # --- VALIDATE (Notificaciones solo en Server como antes) ---
                validate_sales_vs_payments(df_sales, df_payments, name)
                
            logger.info(f'Local {name} processed')
            
    except Exception as e:
        logger.error(e)
        send_email(
            subject="Sales Management - Error de Ejecución",
            body=f"Ocurrió una excepción durante la ejecución del proceso ETL:\n\n{str(e)}"
        )
    finally:
        # 4. Liberación de Recursos
        if db:
            db.close_conection()
        if motor:
            motor.dispose()
            logger.info('Close engine')
        logger.info('End process')

if __name__ == '__main__':
    main()