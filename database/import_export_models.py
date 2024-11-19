from.base import TimeStampedModel
from sqlalchemy import Column, String, Integer, Float, Date, CHAR, Decimal

class ImportExport(TimeStampedModel):
    __tablename__ = 'import_export'


    id = Column(Integer, primary_key=True, autoincrement=True)
    date = Column(Date, nullable=False)
    hs_code = Column(Integer, nullable=False)
    product_description = Column(String(500), nullable=True)
    quantity = Column(Float, nullable=True)
    unit = Column(CHAR(10), nullable=True)
    fob_value_inr = Column(Decimal(20, 2), nullable=True)
    unit_price_inr = Column(Decimal(20, 2), nullable=True)
    fob_value_usd = Column(Decimal(20, 2), nullable=True)
    fob_value_foreign_currency = Column(Decimal(20, 2), nullable=True)
    unit_price_foreign_currency = Column(Decimal(20, 2), nullable=True)
    currency_name = Column(String(100), nullable=True)
    fob_value_in_lacs_inr = Column(Decimal(10, 2), nullable=True)
    iec = Column(String(20), nullable=True)
    indian_exporter_name = Column(String(255), nullable=True)
    exporter_address = Column(String(255), nullable=True)
    exporter_city = Column(String(255), nullable=True)
    pin_code = Column(Integer, nullable=True)
    cha_name = Column(String(255), nullable=True)
    foreign_importer_name = Column(String(255), nullable=True)
    importer_address = Column(String(255), nullable=True)
    importer_country = Column(String(100), nullable=True)
    foreign_port = Column(String(100), nullable=True)
    foreign_country = Column(String(100), nullable=True)
    indian_port = Column(String(100), nullable=True)
    item_no = Column(Integer, nullable=True)
    drawback = Column(String(10), nullable=True)
    chapter = Column(String(10), nullable=True)
    hs_4_digit = Column(String(10), nullable=True)
    month = Column(String(20), nullable=True)
    years = Column(Integer, nullable=True)

    
    