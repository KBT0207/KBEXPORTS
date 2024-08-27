from database.base import Model
from database.connect import engine
from database.import_export_models import *


Model.metadata.create_all(bind=engine)

