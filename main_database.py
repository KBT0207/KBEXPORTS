from database.base import Model
from database.connect import engine
from database.import_export_models import *
from sqlalchemy import create_engine
from sqlalchemy.orm import scoped_session, sessionmaker
from logging_config import logger


Model.metadata.create_all(bind=engine)

class Database:
    def __init__(self, username: str, password: str, host: str, database: str) -> None:
        self.username = username
        self.password = password
        self.host = host
        self.database = database
        self.engine = None
        self.Session = None

    def db_connector(self):
        try:
            connection_string = f"mysql+pymysql://{self.username}:{self.password}@{self.host}/{self.database}"
            self.engine = create_engine(connection_string)
            self.Session = sessionmaker(bind=self.engine)
            logger.info("Connection to the database was successful!")
            return self.Session
        except Exception as e:
            logger.error(f"An error occurred while connecting to the database: {e}")
            return None

    def close(self):
        if self.engine:
            self.engine.dispose()
            logger.info("Database connection closed.")

database1 = Database('root', '1234', "localhost", 'test1')
Session = database1.db_connector()

if Session:
    with Session() as session:
        pass

    database1.close()
