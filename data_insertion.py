import traceback
import sqlalchemy as db
import csv

from pymysql import connect

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

def load_bonds(connection, bonds_data_file=BONDS_PATH):
    """
    ETL function for treasury bond data.
    loads csv data and inserts into Fact_Bond_Prices
    Also Inserts into 'Dim_Bonds' when needed
    """
    # Reset any existing transactions to start fresh
    connection.rollback()
    # Define the fields we’re pulling from the CSV
    fields = ['id', 'treasuryName', 'bond_id', 'date', '1_month', '2_month', '3_month', '6_month', '1_year',
                  '2_year', '3_year', '5_year', '7_year', '10_year', '20_year', '30_year']
    data_frame = pd.read_csv(bonds_data_file, skipinitialspace=True, usecols=fields)
    for index, row in data_frame.iterrows():
        try:
            # Convert row to a dictionary (this will map column names to values)
            row_dict = row.to_dict()
            # Insert date into dim_time
            dim_time_id = upsertTime(connection, row_dict)
            dim_insert = db.text("""
                INSERT INTO Dim_bonds (bond_ID, treasury_name) VALUES (:id, :treasuryName)
            """)
            connection.execute(dim_insert, row_dict)
            # Insert data into fact_bond_prices
            fact_insert = db.text("""
                INSERT INTO Fact_Bond_Prices (time_id, bond_id, one_month, two_month, three_month, six_month, one_year, 
                two_year, three_year, five_year, ten_year, twenty_year, thirty_year)
                VALUES (:time_id, :id, :one_month, :two_month, :three_month, :six_month, :one_year, :two_year, 
                :three_year, :five_year, :ten_year, :twenty_year, :thirty_year)
            """)
            row_dict.update({ "time_id": dim_time_id})
            connection.execute(fact_insert, row_dict)
        except Exception as e:
            print("Error when inserting bonds:", e)
            traceback.print_exc()
    connection.commit()

# load_bonds(connection)
def load_commodities(connection, commodity_data_file=COMMODITIES_PATH):
    """
    ETL function for commodity data.
    Loads CSV data and inserts into 'Fact_Commodity_Prices'
    Also populates Dim_Time and Dim_Commodities as needed
    """
    # Reset any existing transactions to start fresh
    connection.rollback()
    # Define the fields we’re pulling from the CSV
    fields = ['commodity_id', 'commodityName', 'symbol', 'date', 'price']
    data_frame = pd.read_csv(commodity_data_file, skipinitialspace=True, usecols=fields)
    for index, row in data_frame.iterrows():
        try:
            # Convert row to dict
            row_dict = row.to_dict()
            print('Processing row:', row_dict)
            # Retrieve time_id
            dim_time_id = upsertTime(connection, row_dict)
            # Insert commodity into dim_commodities (if doesnt exist)
            select_query = db.text("SELECT commodity_id FROM Dim_Commodities WHERE commodity_id = :commodity_id")
            result = connection.execute(select_query, {'commodity_id': row_dict['commodity_id']}).fetchone()

            if not result:
                dim_insert = db.text("""
                    INSERT INTO Dim_Commodities (commodity_id, commodity_name, commodity_symbol) 
                    VALUES (:commodity_id, :commodityName, :symbol)
                """)
                connection.execute(dim_insert, row_dict)

            # insert fact data
            # TODO: The CSV data we are using seems wrong. Waiting on Ajitesh to send current data files, then fix this
            fact_insert = db.text("""
                INSERT INTO Fact_Commodity_Prices (time_id, commodity_id, price) 
                VALUES (:time_id, :commodity_id, :price)
            """)
            row_dict.update({"time_id", dim_time_id})
            connection.execute(fact_insert, row_dict)
        except Exception as e:
            print("Error when inserting commodities:", e)
            traceback.print_exc()
    connection.commit()


def load_stocks(connection, stock_data_file=STOCKS_PATH):
    """
    ETL function for Stock data.
    Loads CSV data and inserts into 'Fact_Stock_Prices'
    Also populates Dim_Time and Dim_company_statements as needed
    """

    connection.rollback()

    fields = ['id', 'companyName', 'symbol', 'company_id', 'date', 'open', 'high', 'low', 'close', 'adjClose', 'volume', 'unadjustedVolume', 'change', 'changePercentage', 'vwap', 'changeOverTime']

    data_frame = pd.read_csv(stock_data_file, skipinitialspace=True, usecols=fields)
    
    for index, row in data_frame.iterrows():
        try:
            row_dict = row.to_dict()
            rid = row_dict.id
            print('Processing row:', row_dict)

            dim_time_id = upsertTime(connection, row_dict)
            dim_row_exists_qs  = db.text("SELECT * FROM Dim_company_statements WHERE company_id = :id")
            res = connection.execute(dim_row_exists_qs, {'id': rid})
            rows = res.fetchall()
            if len(rows) > 1:
                raise Exception('Ambigious row selection in company statements dimension table')
            if len(rows) == 0:
                raise Exception('No corrisponding row in the company statement dimension table')

            # Transform CSV fields to database fields
            db_row = {
                'time_id': dim_time_id,
                'company_id': rid,
                'open': row_dict['open'],
                'high': row_dict['dayHigh'],
                'low': row_dict['dayLow'],
                'close': row_dict['price'],  # Using current price as close
                'volume': row_dict['volume'],
                'change_percent': row_dict['changePercentage'],
                'adjClose': row_dict['price'],  # Using price as adjClose since we don't have it
                'unadjustedVolume': row_dict['volume'],  # Using regular volume as we don't have unadjusted
                'vwap': None,
                'changeOverTime': None 
            }

            # Prepare fact table insertion
            fact_insert = db.text("""
                INSERT INTO Fact_Stock_Prices (time_id, company_id, open, high, low, close, adj_close, volume, change_percent, vwap) VALUES (
                    :time_id, :company_id, :open, :high, :low, :close,
                    :adjClose, :volume, :change_percent, :vwap
                )
            """)
            
            # Execute the insertion
            connection.execute(fact_insert, db_row)
        except Exception as e:
            print("Error when inserting stocks:", e)
            traceback.print_exc()

    connection.commit()


def load_indexes(connection, indexes_data_file=INDEXES_PATH):
    """
    ETL function for index data.
    Loads CSV data and inserts into Fact_Index_Prices
    Maps fields from CSV format to database schema.
    """
    connection.rollback()
    fields = ['id', 'indexName', 'symbol', 'index_id', 'date', 'price', 
              'changePercentage', 'change', 'dayLow', 'dayHigh', 'yearHigh', 
              'yearLow', 'mktCap', 'exchange', 'volume', 'volumeAvg', 
              'open', 'prevClose']
    
    data_frame = pd.read_csv(indexes_data_file, skipinitialspace=True, usecols=fields)
    
    for index, row in data_frame.iterrows():
        try:
            # Convert row to a dictionary
            row_dict = row.to_dict()
            print('Processing row:', row_dict)
            
            dim_time_id = upsertTime(connection, row_dict)
            
            # Map CSV fields to database fields
            db_row = {
                'time_id': dim_time_id,
                'index_id': row_dict['index_id'],
                'open': row_dict['open'],
                'high': row_dict['dayHigh'],
                'low': row_dict['dayLow'],
                'close': row_dict['price'],  # Using current price as close
                'volume': row_dict['volume'],
                'change_percent': row_dict['changePercentage'],
                'adjClose': row_dict['price'],  # Using price as adjClose since we don't have it
                'unadjustedVolume': row_dict['volume'],  # Using regular volume as we don't have unadjusted
                'vwap': None,  # Can calculate if needed
                'changeOverTime': None  # Can calculate if needed
            }
            
            # Prepare fact table insertion
            fact_insert = db.text("""
                INSERT INTO Fact_Index_Prices (
                    time_id, index_id, open, high, low, close,
                    volume, change_percent, adjClose, unadjustedVolume,
                    vwap, changeOverTime
                ) VALUES (
                    :time_id, :index_id, :open, :high, :low, :close,
                    :volume, :change_percent, :adjClose, :unadjustedVolume,
                    :vwap, :changeOverTime
                )
            """)
            
            # Execute the insertion
            connection.execute(fact_insert, db_row)
            
        except Exception as e:
            print("Error when inserting index data:", e)
            traceback.print_exc()
    
    connection.commit()
# Main exec
if __name__ == "__main__":
    try:
        # Establish connection to db
        with engine.connect() as connection:
            load_bonds(connection)
            load_commodities(connection)
            # load_stocks(connection)
            # load_indexes(connection)
    except Exception as e:
        print("Error in database connection or data loading:", e)
        traceback.print_exc()
