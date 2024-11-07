import traceback
import sqlalchemy as db
import csv

# GLOBALS
BONDS_PATH = '/home/admin/PycharmProjects/Algorithmic-Trading-Warehouse-2024/data/Bonds-data.csv'
COMMODITIES_PATH = '/data/Commodities_data.csv'
STOCKS_PATH = '/data/Stocks_data.csv'
INDEXES_PATH = '/data/Indexes_data.csv'
# Replace username, password, host, dbname with credentials
DATABASE_URI = 'mysql+pymysql://root:FIREWOOD-sack-wino@localhost:3306/ats_dw'

# Initialize connection to db
engine = db.create_engine(DATABASE_URI, echo=True)

print(engine)

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
                # Check if date exists in database
                # TODO: We need to insert time and fix syntax. make function for dim_time insert?
                time_check = db.text("""SELECT * FROM Dim_Time WHERE date = row.date""")
                if time_check.length != 0:
                    time_insert = db.text("""INSERT INTO DIM_TIME (date) VALUES (:row.date)""")
                    time_check = db.text("""SELECT * FROM Dim_Time ORDER BY time_id DESC""")

                time_id = time_check.time_id

                bond_insert = db.text("""
                    INSERT INTO Fact_Bond_Prices (bond_id, one_month, two_month, three_month, six_month, one_year, 
                    two_year, three_year, five_year, ten_year, twenty_year, thirty_year)
                    VALUES (:bond_id, :1_month, :2_month, :3_month, :6_month, :1_year, :2_year, 
                    :3_year, :5_year, :10_year, :20_year, :30_year)
                """)
                connection.execute(bond_insert)
            except Exception as e:
                print("Error when inserting bonds:", e)
                traceback.print_exc()
        connection.commit()


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

if __name__ == "__main__":
    try:
        with engine.connect() as connection:
            load_bonds(connection)
    except Exception as e:
        print("Error in database connection or data loading:", e)
        traceback.print_exc()
