import os
import os
from sqlalchemy import create_engine, Engine, event
from sqlalchemy.orm import scoped_session, sessionmaker
from dotenv import load_dotenv


BASE_DIR = os.path.dirname(os.path.abspath(__file__))

load_dotenv('.env')
DB_USER = os.getenv('DB_USER')
DB_PASS = os.getenv('DB_PASSWORD')
DB_DATABASE1 = os.getenv('DB_DATABASE1')
DB_HOST = os.getenv('DB_HOST')

connection_string = f"mysql+pymysql://{DB_USER}:{DB_PASS}@{DB_HOST}/{DB_DATABASE1}"

engine = create_engine(connection_string)


session = scoped_session(sessionmaker(
    autoflush=False,
    autocommit=False,
    bind=engine
))

@event.listens_for(Engine, 'connect')
def set_mysql_program(dbapi_connection, connection_record):
    cursor = dbapi_connection.cursor()
    cursor.execute("SET SESSION sql_mode='TRADITIONAL'")
    cursor.close()
