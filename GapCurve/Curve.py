import csv
import sqlite3
import pandas as pd
import numpy as np
import math

con = sqlite3.connect('SciCast.db')
c = con.cursor()


c.execute ("DROP TABLE if exists BINARYTrades")
c.execute('''Create table BINARYTrades
              (QuestionID INTEGER,
              Trade_id INTEGER,
              SerSettledVal INTEGER,
              NVL INTEGER)''')

with open('BinaryTradesData.csv', 'r') as f:
    reader = csv.reader(f)
    next(reader)
    binary = list(reader)
c.executemany('INSERT INTO BINARYTrades VALUES (?,?,?,?)', (binary))
con.commit()  

c.execute ("UPDATE BINARYTrades set SerSettledVal=replace(SerSettledVal, '[', '')")
con.commit() 

c.execute ("UPDATE BINARYTrades set SerSettledVal=replace(SerSettledVal, ']', '')")
con.commit() 

c.execute('''DELETE FROM BINARYTrades
          WHERE SerSettledVal between .0000001 and .999999999;'''
    )
con.commit()

c.execute('''ALTER TABLE BINARYTrades
            ADD Gap INTEGER
              ''')
con.commit()

c.execute("UPDATE BINARYTrades set Gap= (SerSettledVal-NVL) ")
con.commit()

c.execute("UPDATE BINARYTrades set NVL= Round(NVL,2) ")
con.commit()

sql= ('''Select NVL, avg(Gap)
            FROM BINARYTrades
            group by NVL
              ''')
curve = c.execute(sql).fetchall()

c.execute ("DROP TABLE if exists Curve")
c.execute('''Create table Curve
              (NVL INTEGER,
              Gap INTEGER
              )''')
con.commit()

c.executemany('INSERT INTO Curve VALUES (?,?)', (curve))
con.commit()

c.execute('''ALTER TABLE Curve
            ADD ForPlot INTEGER
              ''')
con.commit()

c.execute("UPDATE Curve set ForPlot= NVL + Gap ")
con.commit()


c.execute ("DROP TABLE if exists stdevs")
c.execute('''Create table stdevs
              (
              NVL INTEGER, 
              StDev INTEGER,
              SqrtNumRows INTEGER)''')
con.commit()
#Export curve as csv
#Finding Standard deviation
d= 0.01
for r in range(100): 
#Mean
    sql= pd.read_sql_query('''SELECT QuestionID, Trade_id, SerSettledVal, NVL, Gap
            FROM BINARYTrades
              ''', con)
    indexnames = sql[sql['NVL'] < d].index
    sql.drop(indexnames, inplace=True)
    indexnames = sql[sql['NVL'] > d].index
    sql.drop(indexnames, inplace=True)
    sql['ForPlotting'] = sql['Gap']+ sql['NVL']
    average = sql["ForPlotting"].mean()
    #Subtract each number from mean and square result
    sql.insert(6, "AvgGap", average)
    sql= sql.values.tolist()
    c.execute ("DROP TABLE if exists squareddiffs")
    c.execute('''Create table squareddiffs
              (
              questionid INTEGER,
              tradeid INTEGER,
              SerSettledVal INTEGER,
              NVL INTEGER,
              Gap INTEGER,
              ForPlotting INTEGER,
              AvgGap INTEGER)''')
    con.commit()
    c.executemany('INSERT INTO squareddiffs VALUES (?,?,?,?,?,?,?)', (sql))
    con.commit()  
    c.execute('''ALTER TABLE squareddiffs
            ADD diff INTEGER
              ''')
    con.commit()
    c.execute("UPDATE squareddiffs set diff= ForPlotting- AvgGap ")
    con.commit()
    c.execute('''ALTER TABLE squareddiffs
            ADD squareddiff INTEGER
              ''')
    con.commit()
    c.execute("UPDATE squareddiffs set squareddiff= diff*diff ")
    con.commit()    
#get mean of squarred diffs  
    sql= pd.read_sql_query('''Select questionid, tradeid, squareddiff
            FROM squareddiffs
              ''', con)
    numrows = sql.shape[0]
    total= sql['squareddiff'].sum()
    forsd = total/numrows
    sqrtnumrows = np.sqrt(numrows)
    forsd= np.sqrt(forsd)
    data = [[d, forsd, sqrtnumrows]]
    df = pd.DataFrame(data, columns = ['NVL', 'StDev', 'SqrtNumRows'])
    df= df.values.tolist()    
    c.executemany('INSERT INTO stdevs VALUES (?,?,?)', (df))
    con.commit()     
    d= d+ 0.01
    d = round(d,2)
    print(d)

c.execute("UPDATE stdevs set NVL= Round(NVL,2) ")
con.commit()

c.execute("UPDATE Curve set NVL= Round(NVL,2) ")
con.commit()

#Conf Intervals
c.execute ("DROP TABLE if exists ConfIntervalsforCurve")
c.execute('''CREATE TABLE ConfIntervalsforCurve as
          SELECT Curve.NVL, Curve.ForPlot, stdevs.StDev, stdevs.SqrtNumRows
          FROM Curve
          LEFT JOIN stdevs
          ON Curve.NVL=stdevs.NVL''')
con.commit()

c.execute('''ALTER TABLE ConfIntervalsforCurve
            ADD SdoverSqrtNum INTEGER
              ''')
con.commit()
c.execute("UPDATE ConfIntervalsforCurve set SdoverSqrtNum= StDev/SqrtNumRows  ")
con.commit()

c.execute('''ALTER TABLE ConfIntervalsforCurve
            ADD TimesZ INTEGER
              ''')
con.commit()
c.execute("UPDATE ConfIntervalsforCurve set TimesZ= SdoverSqrtNum*1.96 ")
con.commit()

c.execute('''ALTER TABLE ConfIntervalsforCurve
            ADD UpperBound INTEGER
              ''')
con.commit()
c.execute("UPDATE ConfIntervalsforCurve set UpperBound= ForPlot + TimesZ ")
con.commit()

c.execute('''ALTER TABLE ConfIntervalsforCurve
            ADD LowerBound INTEGER
              ''')
con.commit()
c.execute("UPDATE ConfIntervalsforCurve set LowerBound= ForPlot - TimesZ ")
con.commit()

c.execute ("DELETE FROM ConfIntervalsforCurve WHERE NVL IS NULL")
con.commit()