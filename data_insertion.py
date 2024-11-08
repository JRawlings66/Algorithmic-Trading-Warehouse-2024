import traceback
import sqlalchemy as db
import csv
import credentials as c
import pandas as pd

# GLOBALS
BONDS_PATH = '/home/admin/PycharmProjects/Algorithmic-Trading-Warehouse-2024/data/Bonds-data.csv'
COMMODITIES_PATH = 'data/Commodities_data.csv'
STOCKS_PATH = 'data/Stocks_data.csv'
INDEXES_PATH = 'data/Indexes_data.csv'
# Replace username, password, host, dbname with credentials
DATABASE_URI = 'mysql+pymysql://root:FIREWOOD-sack-wino@localhost:3306/ats_dw'

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


# returns time_dim row_id
def upsertTime(connection, date):
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

    time_id = res[0]
    return time_id


def load_bonds(connection, bonds_data_file=BONDS_PATH):
    """
    ETL function for treasury bond data.
    loads csv data and inserts into Fact_Bond_Prices
    TODO: Insertion process for 'Dim_Bonds'
    """
    fields = ['id', 'treasuryName', 'bond_id', 'date', '1_month', '2_month', '3_month', '6_month', '1_year',
                  '2_year', '3_year', '5_year', '7_year', '10_year', '20_year', '30_year']
    data_frame = pd.read_csv(bonds_data_file, skipinitialspace=True, usecols=fields)
    for index, row in data_frame.iterrows():
        try:
            # Convert row to a dictionary (this will map column names to values)
            row_dict = row.to_dict()
            print('row:', row_dict)
            # Insert date into dim_date
            # upsertTime(connection, row)
            dim_insert = db.text("""
                INSERT INTO Dim_Bonds (bond_ID, treasury_name) VALUES (:id, :treasuryName)
            """)
            connection.execute(dim_insert, **row_dict)
            fact_insert = db.text("""
                INSERT INTO Fact_Bond_Prices (time_id, bond_id, one_month, two_month, three_month, six_month, one_year, 
                two_year, three_year, five_year, ten_year, twenty_year, thirty_year)
                VALUES (:time_id, :bond_id, :one_month, :two_month, :three_month, :six_month, :one_year, :two_year, 
                :three_year, :five_year, :ten_year, :twenty_year, :thirty_year)
            """)
            connection.execute(fact_insert, time_id=1, bond_id=row_dict['bond_id'], **row_dict)
        except Exception as e:
            print("Error when inserting bonds:", e)
            traceback.print_exc()


load_bonds(connection)
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
