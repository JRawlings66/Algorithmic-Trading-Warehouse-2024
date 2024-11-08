import traceback
import sqlalchemy as db
import csv
import credentials as c

# GLOBALS
BONDS_PATH = 'raw_data/Bonds_data.csv'
COMMODITIES_PATH = 'raw_data/Commodities_data.csv'
STOCKS_PATH = 'raw_data/Stocks_data.csv'
INDEXES_PATH = 'raw_data/Indexes_data.csv'
# Replace username, password, host, dbname with credentials
DATABASE_URI = 'mysql+pymysql://{c.username}:{c.password}@{c.host}/{c.dbname}'

# Initialize connection to db
engine = db.create_engine(DATABASE_URI, echo=True)


# returns time_dim row_id
def upsertTime(connection, csv_row):
    if csv_row.date == None:
        raise Exception("Row does not contain date data")
    date = csv_row.date

    initial_select_qs = db.text("""SELECT * FROM Dim_Time WHERE DATE = %s""".format(date))
    res = connection.execute(initial_select_qs)

    if res == None:
        insert_qs = db.text("""INSERT INTO Dim_Time (date) VALUES (%s)""".format(date))
        res = connection.execute(insert_qs)
        select_most_recent_qs = db.text("""SELECT TOP 1 * FROM Dim_Time ORDER BY time_id DESC""")
        res = connection.execute(select_most_recent_qs)

    if res == None:
        raise Exception("Unable to Select dim_time id")
    if res.length > 1:
        raise Exception("Ambigious Row selection")

    result = res[0]
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
                # Insert date into dim_date
                upsertTime(connection, row)
                insert_query = db.text("""
                    INSERT INTO Fact_Bond_Prices (bond_id, one_month, two_month, three_month, six_month, one_year, 
                    two_year, three_year, five_year, ten_year, twenty_year, thirty_year)
                    VALUES (:bond_id, :one_month, :two_month, :three_month, :six_month, :one_year, :two_year, 
                    :three_year, :five_year, :ten_year, :twenty_year, :thirty_year)
                """)
                connection.execute(insert_query, **row)
            except Exception as e:
                print("Error when inserting bonds:", e)
                traceback.print_exc()


def load_commodities(connection, commodity_data_raw):
    """
    TODO: ETL function for commodity data.
    """
    with open(COMMODITIES_PATH, "r") as commodity_data:
        csv_loader = csv.DictReader(commodity_data)
        for row in csv_loader:
            try:
                insert_query = db.text("""
                    INSERT INTO Fact_Commodity_Prices (commodity_id, price, change_percentage, 
                    volume) VALUES (:commodity_id, :price, :change_percentage, :volume)
                """)
                connection.execute(insert_query, **row)
            except Exception as e:
                print("Error when inserting commodities:", e)
                traceback.print_exc()


def load_stocks(connection, stock_data_raw):
    """
    TODO: ETL function for stock data.
    """
    with open(STOCKS_PATH, "r") as stock_data:
        csv_loader = csv.DictReader(stock_data)
        for row in csv_loader:
            try:
                insert_query = db.text("""
                    INSERT INTO Fact_Stock_Prices (company_id, open, high, low, close, adj_close, volume) 
                    VALUES (:company_id, :open, :high, :low, :close, :adj_close, :volume)""")
                connection.execute(insert_query, **row)
            except Exception as e:
                print("Error when inserting stocks:", e)
                traceback.print_exc()


def load_indexes(connection, index_data_raw):
    """
    TODO: ETL function for index data.
    """
    with open(INDEXES_PATH, "r") as index_data:
        csv_loader = csv.DictReader(index_data)
        for row in csv_loader:
            try:
                insert_query = db.text("""INSERT INTO Fact_Index_Prices VALUES ()""")
                connection.execute(insert_query, **row)
            except Exception as e:
                print("Error when inserting indexes:", e)
                traceback.print_exc()

# Main exec
if __name__ == "__main__":
    try:
        # Establish connection to db
        with engine.connect() as connection:
            load_bonds(connection)
            # load_commodities(connection)
            # load_stocks(connection)
            # load_indexes(connection)
    except Exception as e:
        print("Error in database connection or data loading:", e)
        traceback.print_exc()