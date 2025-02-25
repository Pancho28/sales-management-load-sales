import os
from sqlalchemy import create_engine, text
import logging

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

class AlchemyConnection:

    def __init__(self, enviroment):
        if enviroment == "dev":
            logger.info('Development enviroment')
            self.db_host = os.getenv("DATABASE_HOST_DEV")
            self.db_user = os.getenv("DATABASE_USERNAME_DEV")
            self.db_password = os.getenv("DATABASE_PASSWORD_DEV")
            self.db_name = os.getenv("DATABASE_NAME_DEV")
            self.db_port = int(os.getenv("DATABASE_PORT_DEV"))
        elif enviroment == 'prod':
            logger.info('Looker enviroment')
            self.db_host = os.getenv("DATABASE_HOST_LOOKER")
            self.db_user = os.getenv("DATABASE_USERNAME_LOOKER")
            self.db_password = os.getenv("DATABASE_PASSWORD_LOOKER")
            self.db_name = os.getenv("DATABASE_NAME_LOOKER")
            self.db_port = int(os.getenv("DATABASE_PORT_LOOKER"))
        else:
            logger.error('Invalid enviroment')
            raise Exception('Invalid enviroment')
        logger.info('Creating engine')
        self.motor = create_engine(f"mysql+pymysql://{self.db_user}:{self.db_password}@{self.db_host}:{self.db_port}/{self.db_name}")
        # Se eliminan los datos de la tabla por_pagar, ya que se carga completa en la ejecucion
        with self.motor.connect() as conexion:
            consulta = text(f"TRUNCATE TABLE por_pagar")
            conexion.execute(consulta)
        logger.info('Truncate por_pagar')
        
    def getMotor(self):
        return self.motor