import sqlalchemy as db



DATABASE_URI = 'mysql+pymysql://Nova:letmein@localhost/testerDB'
engine = db.create_engine(DATABASE_URI, echo=True)
conn = engine.connect()
metadata = db.MetaData()


tester = db.Table('tester', metadata, autoload_with=engine)

inst = db.insert(tester).values(ID="567", Name="Noah")
conn.execute(inst)

query = db.select(tester)

resultsProxy = conn.execute(query)
resultsSets = resultsProxy.fetchall()

print(resultsSets)
conn.commit()