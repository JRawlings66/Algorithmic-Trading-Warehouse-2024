import traceback
import sqlalchemy as db
import csv

# GLOBALS
BONDS_PATH = 'raw_data/Bonds_data.csv'
COMMODITIES_PATH = 'raw_data/Commodities_data.csv'
STOCKS_PATH = 'raw_data/Stocks_data.csv'
INDEXES_PATH = 'raw_data/Indexes_data.csv'
# Replace username, password, host, dbname with credentials
DATABASE_URI = 'mysql+pymysql://username:password@host/dbname'

# Initialize connection to db
engine = db.create_engine(DATABASE_URI, echo=True)


def load_bonds(connection, bonds_data_file=BONDS_PATH):
    """
    ETL function for treasury bond data.
    loads csv data and inserts into Fact_Bond_Prices
    TODO: Insertion process for 'Dim_Bonds'
    """
    with open(bonds_data_file, "r") as bonds_data:
        csv_loader = csv.DictReader(bonds_data)
        for row in csv_loader:
            try:
                insert_query = db.text("""
                    INSERT INTO Fact_Bond_Prices (bond_id, one_month, two_month, three_month, six_month, one_year, 
                    two_year, three_year, five_year, ten_year, twenty_year, thirty_year)
                    VALUES (:bond_id, :one_month, :two_month, :three_month, :six_month, :one_year, :two_year, 
                    :three_year, :five_year, :ten_year, :twenty_year, :thirty_year)
                """)
            except Exception as e:
                print("Error when inserting bonds:", e)
                traceback.print_exc()


def load_commodities(connection, commodity_data_raw):
    """
    TODO: ETL function for commodity data.
    """
    with open(COMMODITIES_PATH, "r") as commodity_data:
        insert_query = db.text("""INSERT INTO Fact_Commodity_Prices VALUES ()`""")
    return None


def load_stocks(connection, stock_data_raw):
    """
    TODO: ETL function for stock data.
    """
    with open(STOCKS_PATH, "r") as stock_data:
        insert_query = db.text("""INSERT INTO Fact_Stock_Prices VALUES ()""")
    return None


def load_indexes(connection, index_data_raw):
    """
    TODO: ETL function for index data.
    """
    with open(INDEXES_PATH, "r") as index_data:
        insert_query = db.text("""INSERT INTO Fact_Index_Prices VALUES ()""")
    return None
