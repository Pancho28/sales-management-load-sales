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

    def get_conections(self):
        conection = pymysql.connect(host=self.db_host, user=self.db_user, password=self.db_password, db=self.db_name, port=self.db_port)
        return conection