from sqlalchemy import Column, DateTime, func
from sqlalchemy.ext.declarative import declarative_base


Model = declarative_base()

class TimeStampedModel(Model):
    __abstract__ = True
    created_at = Column(DateTime, server_default=func.now())
    updated_at = Column(DateTime, server_default= func.now())