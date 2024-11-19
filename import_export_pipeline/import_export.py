import pandas as pd
import numpy as np
from functools import lru_cache
import time
import re
from logging_config import logger

start_time = time.time()



class KayBeeExports:
    start_time = time.time()
    def __init__(self, df: pd.DataFrame) -> None:
        self.df = df.applymap(lambda x: x.upper() if isinstance(x, str) else x)
        self.df.columns = map(str.upper, self.df.columns)
        logger.info("DataFrame initialized and converted to uppercase.")

    def log_time(func):
        def wrapper(self, *args, **kwargs):
            start = time.time()
            logger.info(f"Starting '{func.__name__}' method.")
            result = func(self, *args, **kwargs)
            end = time.time()
            elapsed_time = end - start
            logger.info(f"Finished '{func.__name__}' method in {elapsed_time:.2f} seconds.")
            return result
        return wrapper

    @log_time
    def data_cleaning(self):
        logger.info("Starting data cleaning process.")
        df = self.df.drop_duplicates(['DATE','QUANTITY','INDIAN_EXPORTER_NAME','FOREIGN_IMPORTER_NAME','INDIAN_PORT'])
        logger.info(f"Dropped duplicates, remaining rows: {df.shape[0]}")
        df["DATE"] = pd.to_datetime(df["DATE"], format="%d/%m/%y", errors='coerce').dt.date
        logger.info("Converted 'DATE' column to datetime format.")
        return df

    @log_time
    def product_classification(self)-> pd.DataFrame:
        logger.info("Starting product classification.")
        df = self.data_cleaning()
        df['PRODUCT_CLASSIFIED'] = 'MIXED ITEMS'
        logger.info("Initial classification set to 'MIXED ITEMS'.")
        conditions_updates = [
        (df['PRODUCT_DESCRIPTION'].str.contains(r'\bcoconut\b', case=False, na=False) &
        ~df['PRODUCT_DESCRIPTION'].str.contains(r'\bSLICE|Frozen|cut|DRY|DRIED|POWDER|milk|chunk|JUICE|ice\b', case=False, na=False),
        'COCONUT'),

        (df['PRODUCT_DESCRIPTION'].str.contains(r'\bGarlic\b', case=False, na=False) &
        ~df['PRODUCT_DESCRIPTION'].str.contains(r'\bSLICE|Frozen|cut|DRY|DRIED|POWDER|ice\b', case=False, na=False),
        'FRESH GARLIC'),

        (df['PRODUCT_DESCRIPTION'].str.contains(r'\bMIXED FRUITS|MIXED VEGETABLES|MIX FRUIT|MIX VEGETABLES|MIX VEGITABLE|MIXED VEGETABLES\b', case=False, na=False) &
        ~df['PRODUCT_DESCRIPTION'].str.contains(r'\bSLICE|Frozen|cut|DRY|DRIED|POWDER|milk|chunk|JUICE|ice\b', case=False, na=False),
        'MIX FRUITS & VEG'),

        (df['PRODUCT_DESCRIPTION'].str.contains(r'\bDRUMSTICKS?\b', case=False, na=False) &
        ~df['PRODUCT_DESCRIPTION'].str.contains(r'\bSLICE|Frozen|cut|DRY|DRIED|POWDER|milk|chunk|JUICE|ice\b', case=False, na=False),
        'DRUMSTICK'),

        (df['PRODUCT_DESCRIPTION'].str.contains(r'\bdragon\b', case=False, na=False) &
        ~df['PRODUCT_DESCRIPTION'].str.contains(r'\bSLICE|Frozen|cut|DRY|DRIED|POWDER|milk|chunk|JUICE|ice\b', case=False, na=False),
        'DRAGON FRUITS'),

        (df['PRODUCT_DESCRIPTION'].str.contains(r'\bmango|alphanso\b', case=False, na=False) &
        ~df['PRODUCT_DESCRIPTION'].str.contains(r'\bPulp|SLICE|Frozen|cut mango|pickle|papad|cut|DRY|DRIED|POWDER|pulp|RAW|JUICE\b', case=False, na=False),
        'MANGO'),

        (df['PRODUCT_DESCRIPTION'].str.contains(r'\bBaby|BABYCORN|BABY CORN\b', case=False, na=False) &
        ~df['PRODUCT_DESCRIPTION'].str.contains(r'\bOKRA|POTATO|frozen|IQF|CUT|BRINE|ACETIC|ACID|ONION|BANANAS|BANANA|WHEAT|BABYVITA|BITTER\b', case=False, na=False),
        'BABY CORN'),

        (df['PRODUCT_DESCRIPTION'].str.contains(r'\bPOME|ANAR|POMEGRANATE\b', case=False, na=False) &
        ~df['PRODUCT_DESCRIPTION'].str.contains(r'\bARIL|Pulp|ARILS|DHANA|DANA|frozen|IQF|CUT|BRINE\b', case=False, na=False),
        'POMEGRANATES'),

        (df['PRODUCT_DESCRIPTION'].str.contains(r'\bARIL|POMEGRANATE ARILS|ARILS\b', case=False, na=False) &
        ~df['PRODUCT_DESCRIPTION'].str.contains(r'\bVINEGAR|frozen|IQF|CUT|BRINE|ACETIC|ACID|DRY|DRIED\b', case=False, na=False),
        'POMEGRANATES ARILS'),

        (df['PRODUCT_DESCRIPTION'].str.contains(r'\bOKRA|Lady Finger\b', case=False, na=False) &
        ~df['PRODUCT_DESCRIPTION'].str.contains(r'\bfrozen|IQF|CUT|BRINE|ACETIC|ACID|DRY|DRIED\b', case=False, na=False),
        'OKRA'),

        (df['PRODUCT_DESCRIPTION'].str.contains(r'\bCHILLI|CHILLY\b', case=False, na=False) &
        ~df['PRODUCT_DESCRIPTION'].str.contains(r'\bfrozen|IQF|CUT|BRINE|ACETIC|ACID|DRY|DRIED\b', case=False, na=False),
        'CHILLI'),

        (df['PRODUCT_DESCRIPTION'].str.contains(r'\bGUAVA|PERU\b', case=False, na=False) &
        ~df['PRODUCT_DESCRIPTION'].str.contains(r'\bPULP|IQF|CUT|BRINE|ACETIC|ACID|DRY|DRIED\b', case=False, na=False),
        'GUAVA'),

        (df['PRODUCT_DESCRIPTION'].str.contains(r'\bSAPOTA|CHICKOO\b', case=False, na=False) &
        ~df['PRODUCT_DESCRIPTION'].str.contains(r'\bPULP|SLICE|IQF|CUT|BRINE|ACETIC|ACID|DRY|DRIED|frozen\b', case=False, na=False),
        'CHICKOO'),

        (df['PRODUCT_DESCRIPTION'].str.contains(r'\bDUDHI|BOTTLE GAURD|BOTTLEGAURD\b', case=False, na=False) &
        ~df['PRODUCT_DESCRIPTION'].str.contains(r'\bPULP|SLICE|IQF|CUT|BRINE|ACETIC|ACID|DRY|DRIED|frozen\b', case=False, na=False),
        'DUDHI')
    ]
        for condition, update in conditions_updates:
            matched_rows = df.loc[condition].shape[0]
            logger.info(f"Condition '{update}' matched {matched_rows} rows.")
            df.loc[condition & (df['PRODUCT_CLASSIFIED'] == 'MIXED ITEMS'), 'PRODUCT_CLASSIFIED'] = update
        logger.info("Finished product classification.")
        return df
    
    @log_time
    def calculate_qty(self, description, quantity):
        logger.info(f"Calculating quantity from description: {description}, quantity: {quantity}")
        pattern_kg = r'(\d+(?:\.\d+)?)\s*(?:KG|KGS)\b'
        pattern_other = r'(\d+)\s*(?:G|GM|GX|GMS|GC|GMN)?\s*(?:X|\s)\s*(\d+)\s*(?:PUNNET|MAP\s*BAGS)?'
        pattern_other1 = r'(\d+)[A-Z]+\s*(\d+)'
        pattern_box = r'\b(\d+)\s*B\s*X\s*(\d+)\s*GMS\s*X\s*(\d+)\s*PUNNET\b'
        pattern_non_numeric = r'^\D+$'
        
        try:
            quantity = float(quantity)  # Ensure quantity is a number
            logger.info(f"Quantity converted to float: {quantity}")
        except ValueError:
            logger.error(f"Failed to convert quantity to float: {quantity}")
            return pd.Series([None, None, None, 'none'])

        number1, number2, number3 = None, None, None

        match_other1 = re.search(pattern_other1, description, re.IGNORECASE)
        if match_other1:
            number1 = float(match_other1.group(1))
            number2 = float(match_other1.group(2))
            return pd.Series([number1, number2, None, 'OTHER'])
        
        match_non_numeric = re.match(pattern_non_numeric, description)
        if match_non_numeric:
            return None, None, None, 'NO NUMBER'
        match_kg = re.search(pattern_kg, description, re.IGNORECASE)
        if match_kg:
            number1 = float(match_kg.group(1))
            number2 = 1  # Default to 1 if no second group in KG pattern
            return pd.Series([number1, number2, None, 'KG'])

        match_box = re.search(pattern_box, description, re.IGNORECASE)
        if match_box:
            number1 = int(match_box.group(1))
            number2 = int(match_box.group(2))
            number3 = int(match_box.group(3))
            return pd.Series([number1, number2, number3, 'BOX'])

        match_other = re.search(pattern_other, description, re.IGNORECASE)
        if match_other:
            number1 = int(match_other.group(1))
            number2 = int(match_other.group(2))
            return pd.Series([number1, number2, None, 'OTHER'])
        return pd.Series([None, None, None, 'none'])
   
    @log_time
    @lru_cache(maxsize=None)
    def indian_exporter_classification(self)-> pd.DataFrame:
        logger.info("Starting foreign importer classification.")        
        df = self.product_classification()

        df.loc[df['INDIAN_EXPORTER_NAME'].str.contains(r'^\.+$',case=False,regex=True),'INDIAN_EXPORTER_NAME'] = 'TO ORDER'
        df.loc[df['INDIAN_EXPORTER_NAME'].str.contains(r'^\,\s*$',case=False,regex=True),'INDIAN_EXPORTER_NAME'] = 'TO ORDER'
        df.loc[df['INDIAN_EXPORTER_NAME'].str.contains(r'\bTO ORDER|TO THE(?:\s*[-,]?\s*\w*(?:\s\w*)*)?(?:\s*[-,]?\s*.*)?\b',case=False,regex=True),'FOREIGN_IMPORTER_NAME'] = 'TO ORDER'

        df.loc[df['INDIAN_EXPORTER_NAME'].str.contains(r"(?i)\bKAY\sBEE\b", case=False, regex=True), 'INDIAN_EXPORTER_NAME'] = 'KAY BEE EXPORTS'
        df.loc[df['INDIAN_EXPORTER_NAME'].str.contains(r"(?i)\bMAGNUS\b",case=False,regex=True),'INDIAN_EXPORTER_NAME'] = 'MAGNUS FARM'
        df.loc[df['INDIAN_EXPORTER_NAME'].str.contains(r"(?i)\bFRESHTROP FRUITS\b",case=False,regex=True),'INDIAN_EXPORTER_NAME'] = 'FRESHTROP FRUITS'
        df.loc[df['INDIAN_EXPORTER_NAME'].str.contains(r"(?i)\bGREEN AGREVOLUTION\b",case=False,regex=True),'INDIAN_EXPORTER_NAME'] = 'GREEN AGREVOLUTION'
        df.loc[df['INDIAN_EXPORTER_NAME'].str.contains(r"(?i)\bBARAMATI\b",case=False,regex=True),'INDIAN_EXPORTER_NAME'] = 'BARAMATI AGRO'
        df.loc[df['INDIAN_EXPORTER_NAME'].str.contains(r"(?i)\bULINK AGRITECH\b",case=False,regex=True),'INDIAN_EXPORTER_NAME'] = 'ULINK AGRITECH'
        df.loc[df['INDIAN_EXPORTER_NAME'].str.contains(r"(?i)\bSAM AGRI FRESH\b",case=False,regex=True),'INDIAN_EXPORTER_NAME'] = 'SAM AGRI FRESH'
        df.loc[df['INDIAN_EXPORTER_NAME'].str.contains(r"(?i)\bSANTOSH EXPORTS\b",case=False,regex=True),'INDIAN_EXPORTER_NAME'] = 'SANTOSH EXPORTS'
        df.loc[df['INDIAN_EXPORTER_NAME'].str.contains(r"(?i)\bKASHI EXPORTS\b",case=False,regex=True),'INDIAN_EXPORTER_NAME'] = 'KASHI EXPORTS'
        df.loc[df['INDIAN_EXPORTER_NAME'].str.contains(r"(?i)\bKHUSHI INTERNATIONAL\b",case=False,regex=True),'INDIAN_EXPORTER_NAME'] = 'KHUSHI INTERNATIONAL'
        df.loc[df['INDIAN_EXPORTER_NAME'].str.contains(r"(?i)\bGO GREEN\b",case=False,regex=True),'INDIAN_EXPORTER_NAME'] = 'GO GREEN EXPORT'
        df.loc[df['INDIAN_EXPORTER_NAME'].str.contains(r"(?i)\bTHREE CIRCLES\b",case=False,regex=True),'INDIAN_EXPORTER_NAME'] = 'THREE CIRCLES'
        df.loc[df['INDIAN_EXPORTER_NAME'].str.contains(r"(?i)\bALL SEASON\b",case=False,regex=True),'INDIAN_EXPORTER_NAME'] = 'ALL SEASON EXPORTS'
        df.loc[df['INDIAN_EXPORTER_NAME'].str.contains(r"(?i)\bM. K. EXPORTS\b",case=False,regex=True),'INDIAN_EXPORTER_NAME'] = 'M.K. EXPORTS'
        df.loc[df['INDIAN_EXPORTER_NAME'].str.contains(r"(?i)\bESSAR IMPEX\b",case=False,regex=True),'INDIAN_EXPORTER_NAME'] = 'ESSAR IMPEX'
        df.loc[df['INDIAN_EXPORTER_NAME'].str.contains(r"(?i)\bESSAR EXPORTS\b",case=False,regex=True),'INDIAN_EXPORTER_NAME'] = 'ESSAR EXPORTS'
        df.loc[df['INDIAN_EXPORTER_NAME'].str.contains(r"(?i)\bSUPER FRESH FRUITS\b",case=False,regex=True),'INDIAN_EXPORTER_NAME'] = 'SUPER FRESH FRUIT'
        df.loc[df['INDIAN_EXPORTER_NAME'].str.contains(r"(?i)\bVASHINI EXPORTS\b",case=False,regex=True),'INDIAN_EXPORTER_NAME'] = 'VASHINI EXPORTS'
        df.loc[df['INDIAN_EXPORTER_NAME'].str.contains(r"(?i)\bSCION AGRICOS\b",case=False,regex=True),'INDIAN_EXPORTER_NAME'] = 'SCION AGRICOS'
        df.loc[df['INDIAN_EXPORTER_NAME'].str.contains(r"(?i)\bMANTRA INTERNATIONAL\b",case=False,regex=True),'INDIAN_EXPORTER_NAME'] = 'MANTRA INTERNATIONAL'
        df.loc[df['INDIAN_EXPORTER_NAME'].str.contains(r"(?i)\bSIA IMPEX\b",case=False,regex=True),'INDIAN_EXPORTER_NAME'] = 'SIA IMPEX'

        logger.info("Finished foreign importer classification.")
        return df

    @log_time
    @lru_cache(maxsize=None)
    def foreign_importer_classification(self):
        logger.info("Starting foreign importer classification.")
        df = self.indian_exporter_classification()
        df['FOREIGN_IMPORTER_NAME'] = df['FOREIGN_IMPORTER_NAME'].fillna('TO ORDER')
        df['FOREIGN_IMPORTER_NAME'] = df['FOREIGN_IMPORTER_NAME'].str.replace(r'^\s*\.{1,}\s*(.*\S)\s*\.*$', r'\1', regex=True)
        df.loc[df['FOREIGN_IMPORTER_NAME'].str.contains(r'^\.+$',case=False,regex=True),'FOREIGN_IMPORTER_NAME'] = 'TO ORDER'
        df.loc[df['FOREIGN_IMPORTER_NAME'].str.contains(r'^\,\s*$',case=False,regex=True),'FOREIGN_IMPORTER_NAME'] = 'TO ORDER'
        df.loc[df['FOREIGN_IMPORTER_NAME'].str.contains(r'\bTO ORDER|TO THE(?:\s*[-,]?\s*\w*(?:\s\w*)*)?(?:\s*[-,]?\s*.*)?\b',case=False,regex=True),'FOREIGN_IMPORTER_NAME'] = 'TO ORDER'
        df.loc[df['FOREIGN_IMPORTER_NAME'].str.contains(r"\bWealmoor|Weal Moor\b",case=False,regex=True),'FOREIGN_IMPORTER_NAME'] = 'WEAL MOOR LTD'
        df.loc[df['FOREIGN_IMPORTER_NAME'].str.contains(r"\bFLAMINGO|FLAMINGO PRODUCE\b",case=False,regex=True),'FOREIGN_IMPORTER_NAME'] = 'FLAMINGO PRODUCE'
        df.loc[df['FOREIGN_IMPORTER_NAME'].str.contains(r"\bMINOR|MINOR, WEIR AND WILLIS LIMITED|MINOR WEIR & WILLIS LIMITED, \b",case=False,regex=True),'FOREIGN_IMPORTER_NAME'] = 'MINOR WEIR & WILLIS LIMITED'
        df.loc[df['FOREIGN_IMPORTER_NAME'].str.contains(r"\bNATURE'S PRIDE\b",case=False,regex=True),'FOREIGN_IMPORTER_NAME'] = "NATURE'S PRIDE"
        df.loc[df['FOREIGN_IMPORTER_NAME'].str.contains(r'\byukon\b',case=False,regex=True),'FOREIGN_IMPORTER_NAME'] = 'YUKON INTERNATION'
        df.loc[df['FOREIGN_IMPORTER_NAME'].str.contains(r'\bjalaram produce\b',case=False,regex=True),'FOREIGN_IMPORTER_NAME'] = 'JALARAM PRODUCE'
        df.loc[df['FOREIGN_IMPORTER_NAME'].str.contains(r'\bRaja Foods & Vegetable\b',case=False,regex=True),'FOREIGN_IMPORTER_NAME'] = 'RAJA FOODS & VEGETABLES'
        df.loc[df['FOREIGN_IMPORTER_NAME'].str.contains(r'\bDPS\b',case=False,regex=True),'FOREIGN_IMPORTER_NAME'] = 'DPS'
        df.loc[df['FOREIGN_IMPORTER_NAME'].str.contains(r'\bBARAKAT\b',case=False,regex=True),'FOREIGN_IMPORTER_NAME'] = 'BARAKAT VEGETABLE'
        df.loc[df['FOREIGN_IMPORTER_NAME'].str.contains(r'\bBARFOOTS\b',case=False,regex=True),'FOREIGN_IMPORTER_NAME'] = 'BARFOOTS OF BOTELY'
        df.loc[df['FOREIGN_IMPORTER_NAME'].str.contains(r'\bCORFRESH|COREFRESH\b',case=False,regex=True),'FOREIGN_IMPORTER_NAME'] = 'COREFRESH LTD'
        df.loc[df['FOREIGN_IMPORTER_NAME'].str.contains(r'\bPROVENANCE PARTNERS\b',case=False,regex=True),'FOREIGN_IMPORTER_NAME'] = 'PROVENANCE PARTNERS'
        df.loc[df['FOREIGN_IMPORTER_NAME'].str.contains(r'\bS & F GLOBAL|S&F GLOBAL\b',case=False,regex=True),'FOREIGN_IMPORTER_NAME'] = 'S & F GLOBAL FRESH'
        df.loc[df['FOREIGN_IMPORTER_NAME'].str.contains(r'\bBERRYMOUNT VEGETABLES\b',case=False,regex=True),'FOREIGN_IMPORTER_NAME'] = 'BERRYMOUNT VEGETABLES'
        logger.info("Finished foreign importer classification.")
        return df

    @log_time
    def shipment(self):
        logger.debug('Starting shipment method.')
        df = self.foreign_importer_classification()
        logger.debug(f'Classified foreign importer. DataFrame shape: {df.shape}')
        df['DateQty'] = df.groupby(['DATE', 'FOREIGN_IMPORTER_NAME', 'INDIAN_EXPORTER_NAME','INDIAN_PORT'])['QUANTITY'].transform('sum')
        logger.debug(f'Calculated DateQty column. Sample data: {df["DateQty"].head()}')
        df['Unique For Shipment'] = df['DATE'].astype(str) + df['FOREIGN_IMPORTER_NAME'].astype(str) + df['INDIAN_EXPORTER_NAME'].astype(str) + df['INDIAN_PORT'].astype(str)
        logger.debug(f'Created Unique For Shipment column. Sample data: {df["Unique For Shipment"].head()}')
        df = df.sort_values(by=['DATE', 'INDIAN_EXPORTER_NAME', 'FOREIGN_IMPORTER_NAME'], ascending=[True, True, True])
        logger.debug(f'Sorted DataFrame by date, exporter, and importer.')

        df['SHIFTED'] = df['Unique For Shipment'].shift(-1)
        logger.debug(f'Shifted Unique For Shipment column. Sample data: {df["SHIFTED"].head()}')
        df['SHIPMENT_CHECK'] = np.where(df['Unique For Shipment'] == df['SHIFTED'], 0, 1)
        logger.debug(f'Created SHIPMENT_CHECK column. Sample data: {df["SHIPMENT_CHECK"].head()}')
        return df

    @log_time
    def strip_columns(self):
        df = self.shipment()
        strip_columns = ['INDIAN_EXPORTER_NAME','FOREIGN_IMPORTER_NAME','FOREIGN_COUNTRY', 'UNIT']
        df[strip_columns] = df[strip_columns].apply(lambda x: x.str.strip('.,?*( ) '))
        return df

    @log_time
    def apply_regex(self):
        df = self.strip_columns()
        #some time error value tolist
        results = df.apply(lambda row: self.calculate_qty(row['PRODUCT_DESCRIPTION'], row['QUANTITY']), axis=1)
        df[['number1', 'number2', 'number3', 'UNIT_TYPE']] = pd.DataFrame(results.values.tolist(), index=df.index)
        logger.info("Regex applied to product descriptions and quantities.")
        def net_weight_column_create_condition(row):
            try:
                if row['PRODUCT_CLASSIFIED'] == 'BABY CORN' and row['UNIT_TYPE'] == 'NO NUMBER':
                    logger.info(f"Setting NET WEIGHT to 7 for BABY CORN without number in row: {row.name}")
                    return 7
                if row['PRODUCT_CLASSIFIED'] == 'POMEGRANATES ARILS' and row['UNIT_TYPE'] == 'NO NUMBER':
                    logger.info(f"Setting NET WEIGHT to 14 for POMEGRANATES ARILS without number in row: {row.name}")
                    return 14
                if row['UNIT_TYPE'] == 'KG':
                    return row['number1'] * row['number2']
                elif row['UNIT_TYPE'] == 'BOX':
                    return (float(row['number1']) * float(row['number2']) * float(row['number3'])) / 1000.0
                elif row['UNIT_TYPE'] == 'OTHER':
                    return (float(row['number1']) * float(row['number2'])) / 1000.0
                else:
                    raise ValueError(f"Unknown UNIT_TYPE: {row['UNIT_TYPE']}")
            except (KeyError, ValueError) as e:
                logger.error(f"Error calculating net weight for row {row.name}: {e}")
                return 0
        df['NET WEIGHT'] = df.apply(net_weight_column_create_condition, axis='columns')

        def final_qty_colum_create_condition(row):
            try:
                if row['UNIT'] != 'KGS' and row['UNIT_TYPE'] == 'NO NUMBER'and row['PRODUCT_CLASSIFIED'] == 'BABY CORN':
                    logger.info(f"Adjusting FINAL_QUANTITY for BABY CORN in row: {row.name}")
                    return float(row['QUANTITY']) * 7                

                if row['UNIT'] != 'KGS' and row['UNIT_TYPE'] == 'NO NUMBER' and row['PRODUCT_CLASSIFIED'] == 'POMEGRANATES ARILS':
                    logger.info(f"Adjusting FINAL_QUANTITY for POMEGRANATES ARILS in row: {row.name}")
                    return float(row['QUANTITY']) * 14
                
                if row['UNIT'] == 'LBS':
                    return row['QUANTITY'] * 0.453592 
                
                if row['UNIT'] == 'PRS':
                    return row['QUANTITY'] * 0.453592
                
                if row['UNIT'] == 'QTL':
                    return row['QUANTITY'] * 100

                if row['UNIT'] == 'KGS':
                    return float(row['QUANTITY'])
                elif row['UNIT'] == 'MTS':
                    return float(row['QUANTITY']) * 1000
                elif row['UNIT_TYPE'] == 'KG':
                    return (float(row['number1']) * float(row['number2'])) * float(row['QUANTITY'])
                elif row['UNIT_TYPE'] == 'BOX':
                    return (float(row['number1']) * float(row['number2']) * float(row['number3'])) / 1000.0 * float(row['QUANTITY'])
                elif row['UNIT_TYPE'] == 'OTHER':
                    return (float(row['number1']) * float(row['number2'])) / 1000.0 * float(row['QUANTITY'])
            except ValueError as e:
                logger.error(f"Error calculating final quantity for row {row.name}: {e}")
            return None
        df['FINAL_QUANTITY']=df.apply(final_qty_colum_create_condition, axis='columns')

        def shipment_criteria(row):
            logger.info(f"Processing row: SHIPMENT_CHECK={row['SHIPMENT_CHECK']}, DateQty={row['DateQty']}")
            if (row['SHIPMENT_CHECK'] == 1) and (row['DateQty'] <=3000):
                logger.info("Condition met: SHIPMENT_CHECK=1 and DateQty<=3000, assigning A_SHIPMENT=1")
                return 1
            elif (row['SHIPMENT_CHECK']) == 1 and (row['DateQty'] <=6000):
                logger.info("Condition met: SHIPMENT_CHECK=1 and DateQty<=6000, assigning A_SHIPMENT=2")
                return 2
            elif (row['SHIPMENT_CHECK']) == 1 and (row['DateQty'] <=9000):
                logger.info("Condition met: SHIPMENT_CHECK=1 and DateQty<=9000, assigning A_SHIPMENT=3")
                return 3
            elif (row['SHIPMENT_CHECK']) == 1 and (row['DateQty'] <=12000):
                logger.info("Condition met: SHIPMENT_CHECK=1 and DateQty<=12000, assigning A_SHIPMENT=4")
                return 4
            elif (row['SHIPMENT_CHECK']) == 1 and (row['DateQty'] <=15000):
                logger.info("Condition met: SHIPMENT_CHECK=1 and DateQty<=15000, assigning A_SHIPMENT=5")
                return 5
            elif (row['SHIPMENT_CHECK']) == 1 and (row['DateQty'] <=18000):
                logger.info("Condition met: SHIPMENT_CHECK=1 and DateQty<=18000, assigning A_SHIPMENT=6")
                return 6
            elif (row['SHIPMENT_CHECK']) == 1 and (row['DateQty'] <=21000):
                logger.info("Condition met: SHIPMENT_CHECK=1 and DateQty<=21000, assigning A_SHIPMENT=7")
                return 7
            elif (row['SHIPMENT_CHECK']) == 1 and (row['DateQty'] <=24000):
                logger.info("Condition met: SHIPMENT_CHECK=1 and DateQty<=24000, assigning A_SHIPMENT=8")
                return 8
            else:
                logger.info("Condition not met, assigning A_SHIPMENT=0")
                return 0
        logger.info("Starting to apply 'shipment_criteria' function.")
        df['A_SHIPMENT'] = df.apply(shipment_criteria,axis='columns')
        logger.info("Successfully applied 'shipment_criteria' function.")

        return df

    @log_time
    def product_report(self):
        logger.info("Starting the 'product_report' method.")
        df = self.apply_regex()
        logger.info("apply_regex() method executed successfully.")
        products = ['FRESH GARLIC','COCONUT','BABY CORN','CHICKOO','GUAVA','CHILLI','OKRA','POMEGRANATES','POMEGRANATES ARILS','MANGO']
        product_list = {product: df[df['PRODUCT_CLASSIFIED'] == product] for product in products}
        logger.info(f"Filtered dataframes for products: {', '.join(products)}")
        
        def create_pivot_table(product_df: pd.DataFrame):
            # Create the pivot table
            logger.info(f"Creating pivot table for product dataframe with shape {product_df.shape}.")
            product_pivot = pd.pivot_table(
                product_df,
                index=['INDIAN_EXPORTER_NAME'],
                columns=['FOREIGN_IMPORTER_NAME'],
                values=['FINAL_QUANTITY', 'A_SHIPMENT'],
                aggfunc='sum')
            logger.info("Pivot table created successfully.")

            # Reset index and transform the pivot table
            product_pivot = product_pivot.reset_index().T.reset_index().sort_values(
                by=['FOREIGN_IMPORTER_NAME', 'level_0'],
                ascending=[True, True]
            ).set_index(['FOREIGN_IMPORTER_NAME', 'level_0'])

            # Update column names
            product_pivot.columns = product_pivot.iloc[0]
            product_pivot = product_pivot.iloc[1:]
            product_pivot.reset_index(inplace=True)

            # Replace column names
            product_pivot.replace("A_SHIPMENT", 'NO OF SHIPMENT', inplace=True)
            product_pivot.replace('FINAL_QUANTITY', 'KGS', inplace=True)
            product_pivot.replace('Z', 'TOTAL', inplace=True)

            logger.info("Pivot table transformed and column names updated successfully.")

            return product_pivot

        # Apply the pivot table transformation to each product dataframe
        transformed_product_list = {product: create_pivot_table(df) for product, df in product_list.items()}
        logger.info("Pivot tables created and transformed for all products.")
    
        return transformed_product_list

    @log_time
    def indian_expoprter_pivot(self):
        logger.info('Starting indian_expoprter_pivot method.')
        df = self.apply_regex()
        logger.info('Applied regex transformation to dataframe.')
        indian_exporters = [
            'FRESHTROP FRUITS', 'GREEN AGREVOLUTION', 'BARAMATI AGRO',
            'ULINK AGRITECH', 'SAM AGRI FRESH', 'SANTOSH EXPORTS',
            'KASHI EXPORTS', 'KHUSHI INTERNATIONAL', 'GO GREEN EXPORT', 'THREE CIRCLES', 
            'ALL SEASON EXPORTS', 'M.K. EXPORTS', 'ESSAR IMPEX', 'ESSAR EXPORTS',
            'SUPER FRESH FRUIT', 'VASHINI EXPORTS', 'SCION AGRICOS', 'MANTRA INTERNATIONAL', 'SIA IMPEX'
        ]

        indian_exporter_list = {indian: df[df['INDIAN_EXPORTER_NAME'] == indian] for indian in indian_exporters}
        df['INDIAN_EXPORTER_NAME'] = df['INDIAN_EXPORTER_NAME'].fillna('TO ORDER')
        logger.info('Created individual dataframes for each Indian exporter and filled missing INDIAN_EXPORTER_NAME values.')

        def create_pivot_indian_exporter(indian_exporter_df: pd.DataFrame):
            try:
                logger.info('Creating pivot table for Indian exporter.')
                indian_pivot = pd.pivot_table(indian_exporter_df, columns=['FOREIGN_IMPORTER_NAME'],index=["PRODUCT_CLASSIFIED"],values=['FINAL_QUANTITY', "A_SHIPMENT"], aggfunc='sum')
                indian_pivot = indian_pivot.reset_index().T.reset_index().sort_values(by=['FOREIGN_IMPORTER_NAME', 'level_0'], ascending=[True, True]).set_index(['FOREIGN_IMPORTER_NAME', 'level_0'])

                # Update column names
                indian_pivot.columns = indian_pivot.iloc[0]
                indian_pivot = indian_pivot.iloc[1:]
                indian_pivot.reset_index(inplace=True)

                # Replace column names
                indian_pivot.replace("A_SHIPMENT", 'NO OF SHIPMENT', inplace=True)
                indian_pivot.replace('FINAL_QUANTITY', 'KGS', inplace=True)
                logger.info('Pivot table created and column names updated.')
                return indian_pivot
            except Exception as e:
                logger.error(f'Error while creating pivot table: {e}')
                
   
        
        transformed_indian_exporter_list = {indian: create_pivot_indian_exporter(df) for indian, df in indian_exporter_list.items()}
        logger.info('Pivot tables created for all Indian exporters.')
        return transformed_indian_exporter_list
    
    @log_time
    def importer_pivot(self):
        logger.info('Starting importer_pivot method.')
        df = self.apply_regex()
        logger.info('Applied regex transformation to dataframe.')

        foreign_importer_list = ['WEAL MOOR LTD','FLAMINGO PRODUCE','MINOR WEIR & WILLIS LIMITED',"NATURE'S PRIDE",'YUKON INTERNATION','JALARAM PRODUCE','RAJA FOODS & VEGETABLES']
        logger.info('Created individual dataframes for each foreign importer and filled missing FOREIGN_IMPORTER_NAME values.')
        importer_list = {importer: df[df['FOREIGN_IMPORTER_NAME'] == importer] for importer in foreign_importer_list}
        df['FOREIGN_IMPORTER_NAME'] = df['FOREIGN_IMPORTER_NAME'].fillna('TO ORDER')

        def foreign_importer_pivot(foreign_importr:pd.DataFrame):
            try:
                logger.info('Creating pivot table for foreign importer.')
                importr_pivot = pd.pivot_table(foreign_importr,columns=['INDIAN_EXPORTER_NAME'],index=["PRODUCT_CLASSIFIED"],values=['FINAL_QUANTITY', "A_SHIPMENT"], aggfunc='sum')
                importr_pivot = importr_pivot.reset_index().T.reset_index().sort_values(by=['INDIAN_EXPORTER_NAME', 'level_0'], ascending=[True, True]).set_index(['INDIAN_EXPORTER_NAME', 'level_0'])

                importr_pivot.columns = importr_pivot.iloc[0]
                importr_pivot = importr_pivot.iloc[1:]
                importr_pivot.reset_index(inplace=True)

                # Replace column names
                importr_pivot.replace("A_SHIPMENT", 'NO OF SHIPMENT', inplace=True)
                importr_pivot.replace('FINAL_QUANTITY', 'KGS', inplace=True)
                logger.info('Pivot table created and column names updated.')
                return importr_pivot
            
            except Exception as e:
                logger.error(f'Error while creating pivot table for foreign importer: {e}')
        transformed_importer_list = {importer: foreign_importer_pivot(df) for importer, df in importer_list.items()}
        logger.info('Pivot tables created for all foreign importers.')
        return transformed_importer_list
        
    @log_time   
    def generate_exporter_report(self, file):
        logger.info('Starting generate_exporter_report method.')
        transformed_indian_exporter_list = self.indian_expoprter_pivot()
        logger.info('Transformed Indian exporter data retrieved successfully.')

        def number_to_excel_column(n):
            column = ""
            while n > 0:
                n, remainder = divmod(n - 1, 26)
                column = chr(65 + remainder) + column
            return column

        with pd.ExcelWriter(file, engine='xlsxwriter') as writer:
            logger.info(f'Excel file {file} opened for writing.')
            for indian, df in transformed_indian_exporter_list.items():
                logger.info(f'Processing sheet for Indian exporter: {indian}')
                df.to_excel(writer, sheet_name=indian, index=False)
                workbook  = writer.book
                worksheet = writer.sheets[indian]

                # Apply formatting
                num_rows, num_cols = df.shape
                border_format = workbook.add_format({'border': 1, 'align': 'center', 'valign': 'vcenter'})
                worksheet.conditional_format(f"A1:{number_to_excel_column(num_cols)}{num_rows+1}", {'type': 'no_blanks', 'format': border_format})
                worksheet.conditional_format(f"A1:{number_to_excel_column(num_cols)}{num_rows+1}", {'type': 'blanks', 'format': border_format})
                worksheet.autofit()
                logger.info(f'Formatting applied to sheet {indian}.')

                # Merge cells
                row_values = df['FOREIGN_IMPORTER_NAME'].to_list()
                for i in range(2, num_rows + 2, 2):
                    worksheet.merge_range(f'A{i}:A{i+1}', row_values[i-2], border_format)
                    logger.info(f'Cells merged in sheet {indian} for FOREIGN_IMPORTER_NAME column.')
                logger.info('All sheets processed and saved successfully.')
   
    @log_time
    def generate_product_report(self,file):
        logger.info('Starting generate_product_report method.')
        transformed_product_list = self.product_report()  
        logger.info('Transformed product data retrieved successfully.')

        def number_to_excel_column(n):
            column = ""
            while n > 0:
                n, remainder = divmod(n - 1, 26)
                column = chr(65 + remainder) + column
            return column

        with pd.ExcelWriter(file, engine='xlsxwriter') as writer:
            logger.info(f'Excel file {file} opened for writing.')
            for product, df in transformed_product_list.items():
                logger.info(f'Processing sheet for product: {product}')
                df.to_excel(writer, sheet_name=product, index=False)
                workbook  = writer.book
                worksheet = writer.sheets[product]

                # Apply formatting
                num_rows, num_cols = df.shape

                border_format = workbook.add_format({'border': 1, 'align': 'center', 'valign': 'vcenter'})
                worksheet.conditional_format(f"A1:{number_to_excel_column(num_cols)}{num_rows+1}", {'type': 'no_blanks', 'format': border_format})
                worksheet.conditional_format(f"A1:{number_to_excel_column(num_cols)}{num_rows+1}", {'type': 'blanks', 'format': border_format})
                worksheet.autofit()
                logger.info(f'Formatting applied to sheet {product}.')
                
                row_values = df['FOREIGN_IMPORTER_NAME'].to_list()
                for i in range(2, num_rows + 2, 2):
                    worksheet.merge_range(f'A{i}:A{i+1}', row_values[i-2], border_format)
                logger.info(f'Cells merged in sheet {product} for FOREIGN_IMPORTER_NAME column.')
            logger.info('All sheets processed and saved successfully.') 

    @log_time   
    def generate_importer_report(self, file):
        logger.info('Starting generate_importer_report method.')
        transformed_importer_list = self.importer_pivot()
        logger.info('Transformed importer data retrieved successfully.')

        def number_to_excel_column(n):
            column = ""
            while n > 0:
                n, remainder = divmod(n - 1, 26)
                column = chr(65 + remainder) + column
            return column

        with pd.ExcelWriter(file, engine='xlsxwriter') as writer:
            logger.info(f'Excel file {file} opened for writing.')
            for indian, df in transformed_importer_list.items():
                logger.info(f'Processing sheet for importer: {indian}')
                df.to_excel(writer, sheet_name=indian, index=False)
                workbook  = writer.book
                worksheet = writer.sheets[indian]

                # Apply formatting
                num_rows, num_cols = df.shape
                border_format = workbook.add_format({'border': 1, 'align': 'center', 'valign': 'vcenter'})
                worksheet.conditional_format(f"A1:{number_to_excel_column(num_cols)}{num_rows+1}", {'type': 'no_blanks', 'format': border_format})
                worksheet.conditional_format(f"A1:{number_to_excel_column(num_cols)}{num_rows+1}", {'type': 'blanks', 'format': border_format})
                worksheet.autofit()
                logger.info(f'Formatting applied to sheet {indian}.')

                # Merge cells
                row_values = df['INDIAN_EXPORTER_NAME'].to_list()
                for i in range(2, num_rows + 2, 2):
                    worksheet.merge_range(f'A{i}:A{i+1}', row_values[i-2], border_format)
                    logger.info(f'Cells merged in sheet {indian} for INDIAN_EXPORTER_NAME column.')
                logger.info('All sheets processed and saved successfully.')
    

    



            
