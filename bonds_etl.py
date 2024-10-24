import traceback
import sqlalchemy


# GLOBALS
OUTPUT_FILE_PATH = 'ui/output/Bonds-data.csv'


def load(connection, ):
    
    with open(OUTPUT_FILE_PATH, "r") as bonds_data:
        
        
        
        insert_query = sqlalchemy.text("INSERT INTO `Fact_Bond_Prices` VALUES (:bond_id, :one_month, :two_month, "
                                       ":three_month, :six_month, :one_year, :two_year, :three_year, :five_year, "
                                       ":ten_year, :twenty_year, :thirty_year)")
    
    return None

def main():
    
