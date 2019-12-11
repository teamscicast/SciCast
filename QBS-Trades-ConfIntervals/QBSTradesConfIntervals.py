import csv
import sqlite3
import pandas as pd
import numpy as np

con = sqlite3.connect('SciCast.db')
c = con.cursor()


c.execute ("DROP TABLE if exists QBSOVERTRADES")
c.execute('''Create table QBSOVERTRADES
              (
              TradeNumber INTEGER,
              AverageQBS INTEGER)''')
con.commit()

 
c.execute ("DROP TABLE if exists squarreddiffs")
c.execute('''Create table squarreddiffs
              (
              tradenum INTEGER,
              questionid INTEGER,
              QBS INTEGER,
              AvgQBS)''')
con.commit()

c.execute ("DROP TABLE if exists stdevs")
c.execute('''Create table stdevs
              (
              NumberofTrades INTEGER, 
              StDev INTEGER,
              SqrtNumRows INTEGER)''')
con.commit()

#For first trade
d=2
c.execute ("DROP TABLE if exists TradesIndexed")
c.execute('''Create table TradesIndexed
              (
              Trade_id INTEGER,
              NONZERO INTEGER,
              question_id INTEGER,
              difftime_tradedays INTEGER,
              TBS INTEGER,
              questionduration INTEGER)''')
con.commit()
df2=pd.read_csv("tradesindexed.csv")
df2= df2.drop(df2[df2.NONZERO >=d].index)
df2list= df2.values.tolist()
c.executemany('INSERT INTO TradesIndexed VALUES (?,?,?,?,?,?)', (df2list))
con.commit()  
c.execute ("DELETE FROM TradesIndexed WHERE question_id IS NULL")
con.commit()

c.execute ("DROP TABLE if exists Temp")
c.execute('''Create table Temp
              (
              questionid INTEGER,
              questionduration INTEGER)''')
con.commit()
    
sql = ('''SELECT question_id, sum(difftime_tradedays)
          FROM TradesIndexed
          GROUP BY question_id
             ''')
con.commit()
duration = c.execute(sql).fetchall()

c.executemany('INSERT INTO Temp VALUES (?,?)', (duration))
con.commit()  

c.execute("DROP TABLE if exists Temp2Join")
c.execute('''CREATE TABLE Temp2Join as
          SELECT TradesIndexed.NONZERO, TradesIndexed.question_id, TradesIndexed.difftime_tradedays, TradesIndexed.TBS, Temp.questionduration
          FROM TradesIndexed
          LEFT JOIN Temp
          ON TradesIndexed.question_id=Temp.questionid''')
con.commit()


c.execute('''ALTER TABLE Temp2Join
            ADD QBSindivid INTEGER
              ''')
con.commit()

c.execute("UPDATE Temp2Join set QBSindivid= (TBS * (difftime_tradedays/questionduration)) ")
con.commit()

sql= ('''Select max(NONZERO), question_id, sum(QBSindivid)
            FROM Temp2Join
            group by question_id
              ''')
togroupbyquestion = c.execute(sql).fetchall()
c.execute ("DROP TABLE if exists questiongrouping")
c.execute('''Create table questiongrouping
              (
              tradenum INTEGER,
              questionid INTEGER,
              QBS INTEGER)''')
con.commit()
c.executemany('INSERT INTO questiongrouping VALUES (?,?,?)', (togroupbyquestion))
con.commit() 
d=d-2
sql= pd.read_sql_query('''SELECT tradenum, questionid, QBS
            FROM questiongrouping
              ''', con)
grouping=pd.DataFrame(sql, columns=['tradenum', 'questionid', 'QBS'])
indexnames = grouping[grouping['tradenum'] <= d].index
grouping.drop(indexnames, inplace=True)
c.execute ("DROP TABLE if exists questiongrouping")
c.execute('''Create table questiongrouping
              (
              tradenum INTEGER,
              questionid INTEGER,
              QBS INTEGER)''')
con.commit()
grouping= grouping.values.tolist()
c.executemany('INSERT INTO questiongrouping VALUES (?,?,?)', (grouping))
con.commit()
    
#Question Grouping is table with valid QBS Scores for specific loop
sql= ('''Select max(tradenum), avg(QBS)
            FROM questiongrouping
              ''')
average = c.execute(sql).fetchall()
c.executemany('INSERT INTO QBSOVERTRADES VALUES (?,?)', (average))
con.commit()   
    
#CALCULATE STANDARD DEVIATION OF QBS AT 1
    #Calculate mean
sql= pd.read_sql_query('''SELECT TradeNumber, AverageQBS
            FROM QBSOVERTRADES
              ''', con)
formean=pd.DataFrame(sql, columns=['TradeNumber', 'AverageQBS'])
d=1
indexnames = formean[formean['TradeNumber'] != d].index
formean.drop(indexnames, inplace=True)
formean = formean.reset_index(drop=True)
formean['AverageQBS'] = formean['AverageQBS'].astype(float)
mean = formean.at[0, 'AverageQBS']
    #Subtract each number from mean and square result
sql= pd.read_sql_query('''SELECT tradenum, questionid, QBS
            FROM questiongrouping
              ''', con)
insertingdata=pd.DataFrame(sql, columns=['tradenum', 'questionid', 'QBS'])
insertingdata.insert(3, "AvgQBS", mean) 
insertingdata= insertingdata.values.tolist()
c.execute ("DROP TABLE if exists squarreddiffs")
c.execute('''Create table squarreddiffs
              (
              tradenum INTEGER,
              questionid INTEGER,
              QBS INTEGER,
              AvgQBS)''')
con.commit()
c.executemany('INSERT INTO squarreddiffs VALUES (?,?,?,?)', (insertingdata))
con.commit()  
c.execute('''ALTER TABLE squarreddiffs
            ADD diff INTEGER
              ''')
con.commit()
c.execute("UPDATE squarreddiffs set diff= QBS- AvgQBS ")
con.commit()
c.execute('''ALTER TABLE squarreddiffs
            ADD squareddiff INTEGER
              ''')
con.commit()
c.execute("UPDATE squarreddiffs set squareddiff= diff*diff ")
con.commit()
sql= pd.read_sql_query('''Select tradenum, questionid, squareddiff
            FROM squarreddiffs
              ''', con)
numrows = sql.shape[0]
tradenums = sql['tradenum'].max()
total= sql['squareddiff'].sum()
forsd = total/numrows
sqrtnumrows = np.sqrt(numrows)
data = [[tradenums, forsd, sqrtnumrows]]
forsd= np.sqrt(forsd)
df = pd.DataFrame(data, columns = ['TradeNum', 'StDev', 'SqrtNumRows'])
df= df.values.tolist()
c.executemany('INSERT INTO stdevs VALUES (?,?,?)', (df))
con.commit()   
print(d)


#Loop run for rest of trades
d=6
while d in range(1000): 
    c.execute ("DROP TABLE if exists TradesIndexed")
    c.execute('''Create table TradesIndexed
              (
              Trade_id INTEGER,
              NONZERO INTEGER,
              question_id INTEGER,
              difftime_tradedays INTEGER,
              TBS INTEGER,
              questionduration INTEGER)''')
    con.commit()
    df2=pd.read_csv("tradesindexed.csv")
    df2= df2.drop(df2[df2.NONZERO >=d].index)
    df2list= df2.values.tolist()
    c.executemany('INSERT INTO TradesIndexed VALUES (?,?,?,?,?,?)', (df2list))
    con.commit()  
    c.execute ("DELETE FROM TradesIndexed WHERE question_id IS NULL")
    con.commit()

    c.execute ("DROP TABLE if exists Temp")
    c.execute('''Create table Temp
              (
              questionid INTEGER,
              questionduration INTEGER)''')
    con.commit()
    
    sql = ('''SELECT question_id, sum(difftime_tradedays)
          FROM TradesIndexed
          GROUP BY question_id
             ''')
    con.commit()
    duration = c.execute(sql).fetchall()

    c.executemany('INSERT INTO Temp VALUES (?,?)', (duration))
    con.commit()  

    c.execute("DROP TABLE if exists Temp2Join")
    c.execute('''CREATE TABLE Temp2Join as
          SELECT TradesIndexed.NONZERO, TradesIndexed.question_id, TradesIndexed.difftime_tradedays, TradesIndexed.TBS, Temp.questionduration
          FROM TradesIndexed
          LEFT JOIN Temp
          ON TradesIndexed.question_id=Temp.questionid''')
    con.commit()


    c.execute('''ALTER TABLE Temp2Join
            ADD QBSindivid INTEGER
              ''')
    con.commit()

    c.execute("UPDATE Temp2Join set QBSindivid= (TBS * (difftime_tradedays/questionduration)) ")
    con.commit()

    sql= ('''Select max(NONZERO), question_id, sum(QBSindivid)
            FROM Temp2Join
            group by question_id
              ''')
    togroupbyquestion = c.execute(sql).fetchall()
    c.execute ("DROP TABLE if exists questiongrouping")
    c.execute('''Create table questiongrouping
              (
              tradenum INTEGER,
              questionid INTEGER,
              QBS INTEGER)''')
    con.commit()
    c.executemany('INSERT INTO questiongrouping VALUES (?,?,?)', (togroupbyquestion))
    con.commit() 
    d=d-4
    sql= pd.read_sql_query('''SELECT tradenum, questionid, QBS
            FROM questiongrouping
              ''', con)
    grouping=pd.DataFrame(sql, columns=['tradenum', 'questionid', 'QBS'])
    indexnames = grouping[grouping['tradenum'] <= d].index
    grouping.drop(indexnames, inplace=True)
    c.execute ("DROP TABLE if exists questiongrouping")
    c.execute('''Create table questiongrouping
              (
              tradenum INTEGER,
              questionid INTEGER,
              QBS INTEGER)''')
    con.commit()
    grouping= grouping.values.tolist()
    c.executemany('INSERT INTO questiongrouping VALUES (?,?,?)', (grouping))
    con.commit()
    
#Question Grouping is table with valid QBS Scores for specific loop
    sql= ('''Select max(tradenum), avg(QBS)
            FROM questiongrouping
              ''')
    average = c.execute(sql).fetchall()
    c.executemany('INSERT INTO QBSOVERTRADES VALUES (?,?)', (average))
    con.commit()   

#CALCULATE STANDARD DEVIATION OF QBS AT EACH TRADE
    #Calculate mean
    sql= pd.read_sql_query('''SELECT TradeNumber, AverageQBS
            FROM QBSOVERTRADES
              ''', con)
    formean=pd.DataFrame(sql, columns=['TradeNumber', 'AverageQBS'])
    d=d+3
    indexnames = formean[formean['TradeNumber'] != d].index
    formean.drop(indexnames, inplace=True)
    formean = formean.reset_index(drop=True)
    formean['AverageQBS'] = formean['AverageQBS'].astype(float)
    mean = formean.at[0, 'AverageQBS']
    #Subtract each number from mean and square result
    sql= pd.read_sql_query('''SELECT tradenum, questionid, QBS
            FROM questiongrouping
              ''', con)
    insertingdata=pd.DataFrame(sql, columns=['tradenum', 'questionid', 'QBS'])
    insertingdata.insert(3, "AvgQBS", mean) 
    insertingdata= insertingdata.values.tolist()
    c.execute ("DROP TABLE if exists squareddiffs")
    c.execute('''Create table squareddiffs
              (
              tradenum INTEGER,
              questionid INTEGER,
              QBS INTEGER,
              AvgQBS)''')
    con.commit()
    c.executemany('INSERT INTO squareddiffs VALUES (?,?,?,?)', (insertingdata))
    con.commit()  
    c.execute('''ALTER TABLE squareddiffs
            ADD diff INTEGER
              ''')
    con.commit()
    c.execute("UPDATE squareddiffs set diff= QBS- AvgQBS ")
    con.commit()
    c.execute('''ALTER TABLE squareddiffs
            ADD squareddiff INTEGER
              ''')
    con.commit()
    c.execute("UPDATE squareddiffs set squareddiff= diff*diff ")
    con.commit()
#get mean of squarred diffs   
    sql= pd.read_sql_query('''Select tradenum, questionid, squareddiff
            FROM squareddiffs
              ''', con)
    numrows = sql.shape[0]
    tradenums = sql['tradenum'].max()
    total= sql['squareddiff'].sum()
    forsd = total/numrows
    sqrtnumrows = np.sqrt(numrows)
    forsd= np.sqrt(forsd)
    data = [[tradenums, forsd, sqrtnumrows]]
    df = pd.DataFrame(data, columns = ['TradeNum', 'StDev', 'SqrtNumRows'])
    df= df.values.tolist()
    c.executemany('INSERT INTO stdevs VALUES (?,?,?)', (df))
    con.commit()   
    d=d+6
    print(d)


#Conf Intervals
c.execute ("DROP TABLE if exists ConfIntervalsforQBSOverTrade")
c.execute('''CREATE TABLE ConfIntervalsforQBSOverTrade as
          SELECT stdevs.NumberofTrades, QBSOVERTRADES.AverageQBS, stdevs.StDev, stdevs.SqrtNumRows
          FROM QBSOVERTRADES
          LEFT JOIN stdevs
          ON QBSOVERTRADES.TradeNumber=stdevs.NumberofTrades''')
con.commit()

c.execute('''ALTER TABLE ConfIntervalsforQBSOverTrade
            ADD SdoverSqrtNum INTEGER
              ''')
con.commit()
c.execute("UPDATE ConfIntervalsforQBSOverTrade set SdoverSqrtNum= StDev/SqrtNumRows ")
con.commit()

c.execute('''ALTER TABLE ConfIntervalsforQBSOverTrade
            ADD TimesZ INTEGER
              ''')
con.commit()
c.execute("UPDATE ConfIntervalsforQBSOverTrade set TimesZ= SdoverSqrtNum*1.96 ")
con.commit()

c.execute('''ALTER TABLE ConfIntervalsforQBSOverTrade
            ADD UpperBound INTEGER
              ''')
con.commit()
c.execute("UPDATE ConfIntervalsforQBSOverTrade set UpperBound= AverageQBS + TimesZ ")
con.commit()

c.execute('''ALTER TABLE ConfIntervalsforQBSOverTrade
            ADD LowerBound INTEGER
              ''')
con.commit()
c.execute("UPDATE ConfIntervalsforQBSOverTrade set LowerBound= AverageQBS - TimesZ ")
con.commit()

#The final QBS over trade data with confidence intervals can be found in table "ConfIntervalsforQBSOverTrade"

#Calculating count of questions
c.execute ("DROP TABLE if exists Count")
c.execute('''Create table Count
              (
              TradeNum INTEGER,
              NumofQuestions INTEGER
)''')
con.commit()
d=0
while d in range(1000): 
    c.execute ("DROP TABLE if exists TradesIndexed")
    c.execute('''Create table TradesIndexed
              (
              Trade_id INTEGER,
              NONZERO INTEGER,
              question_id INTEGER,
              difftime_tradedays INTEGER,
              TBS INTEGER,
              questionduration INTEGER)''')
    con.commit()
    with open('tradesindexed.csv', 'r') as f:
        reader = csv.reader(f)
        next(reader)
        trades = list(reader)
    c.executemany('INSERT INTO TradesIndexed VALUES (?,?,?,?,?,?)', (trades))
    con.commit()  
    sql= pd.read_sql_query('''Select Trade_id, NONZERO, question_id, difftime_tradedays, TBS, questionduration
            FROM TradesIndexed
              ''', con)
    d=d+1
    indexnames = sql[sql['NONZERO'] <= d].index
    sql.drop(indexnames, inplace=True)
    sql = sql.reset_index(drop=True)
    unique = sql.question_id.nunique()
    d = d-1
    data = [[d, unique]]
    df = pd.DataFrame(data, columns = ['TradeNum', 'NumofQuestions'])
    df= df.values.tolist()
    c.executemany('INSERT INTO Count VALUES (?,?)', (df))
    con.commit()   
    d = d+50
    print(d)

