from sqlalchemy import Column,String,Integer,Float,Date
from.base import TimeStampedModel

class ImportExport(TimeStampedModel):
    __tablename__ = 'import_export'

    id = Column(Integer,primary_key=True,autoincrement=True)
    date = Column(Date,nullable=False)
    
    