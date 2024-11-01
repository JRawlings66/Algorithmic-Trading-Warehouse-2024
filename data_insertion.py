import traceback
import sqlalchemy


# GLOBALS
OUTPUT_FILE_PATH = 'ui/output/Bonds-data.csv'


def load_bonds(connection, bond_data_raw):
    '''
    ETL function for treasury bond data.
    param:
    return:
    '''
    with open(OUTPUT_FILE_PATH, "r") as bonds_data:
        insert_query = sqlalchemy.text("INSERT INTO `Fact_Bond_Prices` VALUES (:bond_id, :one_month, :two_month, "
                                       ":three_month, :six_month, :one_year, :two_year, :three_year, :five_year, "
                                       ":ten_year, :twenty_year, :thirty_year)")
    return None

def load_commodities(connection, commodity_data_raw):
    '''
    ETL function for commodity data.
    param:
    return:
    '''
    with open(OUTPUT_FILE_PATH, "r") as commodity_data:
        insert_query = sqlalchemy.text("INSERT INTO `Fact_Commodity_Prices VALUES ()`")
    return None

def load_stocks(connection, stock_data_raw):
    '''
    ETL function for stock data.
    param:
    return:
    '''
    with open(OUTPUT_FILE_PATH, "r") as stock_data:
        insert_query = sqlalchemy.text("INSERT INTO `Fact_Stock_Prices VALUES ()")
    return None

def load_indexes(connection, index_data_raw):
    '''
    ETL function for index data.
    param:
    return:
    '''
    with open(OUTPUT_FILE_PATH, "r") as index_data:
        insert_query = sqlalchemy.text("INSERT INTO `Fact_Index_Prices VALUES ()")
    return None
