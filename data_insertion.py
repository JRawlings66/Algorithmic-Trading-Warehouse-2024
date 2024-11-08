import traceback
import sqlalchemy as db
import csv
import credentials as c
import json

# GLOBALS
BONDS_PATH = 'data/Bonds-data.csv'
COMMODITIES_PATH = 'data/Commodities-data.csv'
STOCKS_PATH = 'data/Stocks-data.csv'
INDEXES_PATH = 'data/Indexes-data.csv'
# Replace username, password, host, dbname with credentials
DATABASE_URI = f'mysql+pymysql://{c.username}:{c.password}@{c.host}/{c.dbname}'

# Initialize connection to db
engine = db.create_engine(DATABASE_URI, echo=True)
connection = engine.connect()

#returns time_dim row_id
def upsertTime(connection, csv_row):
    if csv_row['date'] == None:
        raise Exception("Row does not contain date data")
    date = csv_row['date']
    
    initial_select_qs = db.text("""SELECT * FROM Dim_Time WHERE DATE = :date""")
    res = connection.execute(initial_select_qs, {'date': date})
    rows = res.fetchall()

    if len(rows) == 0:
        print('if triggered')
        insert_qs = db.text("""INSERT INTO Dim_Time (date) VALUES (:date)""")
        connection.execute(insert_qs, {'date': date})
        select_most_recent_qs = db.text("""SELECT * FROM Dim_Time ORDER BY time_id DESC LIMIT 1""")
        res = connection.execute(select_most_recent_qs)

    rows = res.fetchall()

    if len(rows) == 0:
        raise Exception("Unable to Select dim_time id")
    if len(rows) > 1:
        raise Exception("Ambigious Row selection")

    result = rows[0]
    return result


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
                time_id = upsertTime(connection, row)

                insert_query = db.text("""
                    INSERT INTO Fact_Bond_Prices (bond_id, one_month, two_month, three_month, six_month, one_year, 
                    two_year, three_year, five_year, ten_year, twenty_year, thirty_year)
                    VALUES (:bond_id, :one_month, :two_month, :three_month, :six_month, :one_year, :two_year, 
                    :three_year, :five_year, :ten_year, :twenty_year, :thirty_year)
                """)
            except Exception as e:
                print("Error when inserting bonds:", e)
                traceback.print_exc()


load_bonds(connection)
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

