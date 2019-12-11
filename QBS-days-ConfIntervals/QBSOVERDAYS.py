import csv
import sqlite3
import pandas as pd
import numpy as np

con = sqlite3.connect('SciCast.db')
c = con.cursor()

df=pd.read_csv("TradesIndexCorrectDuration.csv")
df["SumofPrevious"] = np.nan
df.loc[0,'SumofPrevious'] = df.loc[0,'tradeduration']
for i in range(1, len(df)):
    if df.loc[i,'question_id'] != df.loc[i-1, 'question_id']:
        df.loc[i,'SumofPrevious'] = df.loc[i,'tradeduration']
    if df.loc[i,'question_id'] == df.loc[i-1, 'question_id']:
        df.loc[i,'SumofPrevious'] = df.loc[i-1,'SumofPrevious'] + df.loc[i,'tradeduration']
df.drop(['create_trade', 'update_trade', 'difftime_questiondays'], axis=1)
df= df.values.tolist()
c.execute ("DROP TABLE if exists daysums")
c.execute('''Create table daysums
              (trade_id INTEGER,
              tradeindex INTEGER,
              question_id INTEGER,j
              create_trade DATETIME,
              update_trade DATETIME, 
              tradeduration INTEGER,
              TBS INTEGER,
              difftime_questiondays INTEGER,
              QBS INTEGER,
              SumofPrevious INTEGER)''')
con.commit()
c.executemany('INSERT INTO daysums VALUES (?,?,?,?,?,?,?,?,?,?)', (df))
con.commit()

c.execute ("DROP TABLE if exists sums")
c.execute('''Create table sums
              (
              question INTEGER,
              questionduration INTEGER)''')
con.commit()
with open('QuestionDurationsforTrade.csv', 'r') as f:
    reader = csv.reader(f)
    next(reader)
    sums = list(reader)
c.executemany('INSERT INTO sums VALUES (?,?)', (sums))
con.commit() 

c.execute ("DROP TABLE if exists dayandquestsum")
c.execute('''CREATE TABLE dayandquestsum as
          SELECT daysums.question_id, daysums.TBS, daysums.QBS, daysums.tradeduration, daysums.SumofPrevious, sums.questionduration
          FROM daysums
          LEFT JOIN sums
          ON daysums.question_id=sums.question''')
con.commit()

c.execute('''ALTER TABLE dayandquestsum
            ADD DaystoClose
              ''')
con.commit()

c.execute("UPDATE dayandquestsum set DaystoClose= (questionduration - SumofPrevious) ")
con.commit()

#Export dayandquestsum as csv

c.execute ("DROP TABLE if exists QBSOVERDAYS")
c.execute('''Create table QBSOVERDAYS
              (
              DaysTilClose INTEGER,
              AverageQBS INTEGER)''')
con.commit()

c.execute ("DROP TABLE if exists stdevs")
c.execute('''Create table stdevs
              (
              DaysTilClose INTEGER, 
              StDev INTEGER,
              SqrtNumRows INTEGER)''')
con.commit()

d= 5
while d in range(300): 
    c.execute ("DROP TABLE if exists calcoverdays")
    c.execute('''Create table calcoverdays
              (
              question_id INTEGER,
              TBS INTEGER,
              QBS INTEGER,
              tradeduration,
              SumofPrevious INTEGER,
              questionduration INTEGER,
              Daystoclose INTEGER)''')
    con.commit()
    df2=pd.read_csv("dayandquestsum.csv")
    df2= df2.drop(df2[df2.DaystoClose <=d].index)
    df2list= df2.values.tolist()
    c.executemany('INSERT INTO calcoverdays VALUES (?,?,?,?,?,?,?)', (df2list))
    con.commit()  
    c.execute ("DELETE FROM calcoverdays WHERE question_id IS NULL")
    con.commit()
    c.execute ("DROP TABLE if exists Temp")
    c.execute('''Create table Temp
              (
              questionid INTEGER,
              questiondurationfordays INTEGER)''')
    con.commit()
    
    sql = ('''SELECT question_id, sum(tradeduration)
          FROM calcoverdays
          GROUP BY question_id
             ''')
    con.commit()
    duration = c.execute(sql).fetchall()

    c.executemany('INSERT INTO Temp VALUES (?,?)', (duration))
    con.commit()  

    c.execute("DROP TABLE if exists Temp2Join")
    c.execute('''CREATE TABLE Temp2Join as
          SELECT calcoverdays.question_id, calcoverdays.tradeduration, calcoverdays.SumofPrevious,calcoverdays.TBS, calcoverdays.Daystoclose, Temp.questiondurationfordays
          FROM calcoverdays
          LEFT JOIN Temp
          ON calcoverdays.question_id=Temp.questionid''')
    con.commit()

    c.execute('''ALTER TABLE Temp2Join
            ADD QBSindivid INTEGER
              ''')
    con.commit()

    c.execute("UPDATE Temp2Join set QBSindivid= (TBS * (tradeduration/questiondurationfordays)) ")
    con.commit()

    sql= ('''Select min(Daystoclose), question_id, sum(QBSindivid)
            FROM Temp2Join
            group by question_id
              ''')
    togroupbyquestion = c.execute(sql).fetchall()
    c.execute ("DROP TABLE if exists questiongrouping")
    c.execute('''Create table questiongrouping
              (
              DaysTilClose INTEGER,
              questionid INTEGER,
              QBS INTEGER)''')
    con.commit()
    c.executemany('INSERT INTO questiongrouping VALUES (?,?,?)', (togroupbyquestion))
    con.commit() 
#Question Grouping is table with valid QBS Scores for specific loop   
    sql= ('''Select min(DaysTilClose), avg(QBS)
            FROM questiongrouping
              ''')
    average = c.execute(sql).fetchall()
    c.executemany('INSERT INTO QBSOVERDAYS VALUES (?,?)', (average))
    con.commit()  
    c.execute("UPDATE QBSOVERDAYS set DaysTilClose= Round(DaysTilClose,0) ")
    con.commit()
    
#CALCULATE STANDARD DEVIATION OF QBS AT EACH TRADE
    #Calculate mean
    sql= pd.read_sql_query('''SELECT DaysTilClose , AverageQBS
            FROM QBSOVERDAYS
              ''', con)
    formean=pd.DataFrame(sql, columns=['DaysTilClose', 'AverageQBS'])
    indexnames = formean[formean['DaysTilClose'] != d].index
    formean.drop(indexnames, inplace=True)
    formean = formean.reset_index(drop=True)
    formean['AverageQBS'] = formean['AverageQBS'].astype(float)
    mean = formean.at[0, 'AverageQBS']
    #Subtract each number from mean and square result
    sql= pd.read_sql_query('''SELECT DaysTilClose, questionid, QBS
            FROM questiongrouping
              ''', con)
    insertingdata=pd.DataFrame(sql, columns=['DaysTilClose', 'questionid', 'QBS'])
    insertingdata.insert(3, "AvgQBS", mean) 
    insertingdata= insertingdata.values.tolist()
    c.execute ("DROP TABLE if exists squareddiffs")
    c.execute('''Create table squareddiffs
              (
              DaysTilClose INTEGER,
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

#get mean of squareddiffs 
    sql= pd.read_sql_query('''SELECT DaysTilClose, questionid, squareddiff
                           FROM squareddiffs''', con)
    numrows= sql.shape[0] 
    days = sql['DaysTilClose'].min()
    days = round(days,0)
    days = int(days)
    total = sql['squareddiff'].sum()
    forsd = total/numrows
    sqrtnumrows=np.sqrt(numrows)
    forsd = np.sqrt(forsd)
    data = [[days, forsd, sqrtnumrows]]
    df = pd.DataFrame(data, columns = ['days', 'StDev', 'SqrtNumRows'])
    df=df.values.tolist()
    c.executemany('INSERT INTO stdevs VALUES (?,?,?)', (df))
    con.commit() 
    d=d+5
    print(d)


#Conf Intervals
c.execute ("DROP TABLE if exists ConfIntervalsforQBSOverDays")
c.execute('''CREATE TABLE ConfIntervalsforQBSOverDays as
          SELECT stdevs.DaysTilClose, QBSOVERDays.AverageQBS, stdevs.StDev, stdevs.SqrtNumRows
          FROM QBSOVERDays
          LEFT JOIN stdevs
          ON QBSOVERDays.DaysTilClose=stdevs.DaysTilClose''')
con.commit()

c.execute('''ALTER TABLE ConfIntervalsforQBSOverDays
            ADD SDoverSqrtNum INTEGER
              ''')
con.commit()
c.execute("UPDATE ConfIntervalsforQBSOverDays set SDoverSqrtNum= StDev/SqrtNumRows ")
con.commit()

c.execute('''ALTER TABLE ConfIntervalsforQBSOverDays
            ADD TimesZ INTEGER
              ''')
con.commit()
c.execute("UPDATE ConfIntervalsforQBSOverDays set TimesZ= SDoverSqrtNum*1.96 ")
con.commit()

c.execute('''ALTER TABLE ConfIntervalsforQBSOverDays
            ADD UpperBound INTEGER
              ''')
con.commit()
c.execute("UPDATE ConfIntervalsforQBSOverDays set UpperBound= AverageQBS + TimesZ")
con.commit()

c.execute('''ALTER TABLE ConfIntervalsforQBSOverDays
            ADD LowerBound INTEGER
              ''')
con.commit()
c.execute("UPDATE ConfIntervalsforQBSOverDays set LowerBound= AverageQBS - TimesZ ")
con.commit()

#The final QBS over days data with confidence intervals can be found in table "ConfIntervalsforQBSOverDays"



#Calculating count of questions
c.execute ("DROP TABLE if exists Count")
c.execute('''Create table Count
              (
              Days INTEGER,
              NumofQuestions INTEGER
)''')
con.commit()
d=0
while d in range(281): 
    c.execute ("DROP TABLE if exists Trades")
    c.execute('''Create table Trades
              (
              question_id INTEGER,
              TBS INTEGER,
              QBS INTEGER,
              tradeduration INTEGER,
              SumofPrevious INTEGER,
              questionduration INTEGER,
              DaystoClose INTEGER)''')
    con.commit()
    with open('dayandquestsum.csv', 'r') as f:
        reader = csv.reader(f)
        next(reader)
        trades = list(reader)
    c.executemany('INSERT INTO Trades VALUES (?,?,?,?,?,?,?)', (trades))
    con.commit()  
    sql= pd.read_sql_query('''Select question_id, TBS, QBS, tradeduration, SumofPrevious, questionduration, DaystoClose
            FROM Trades
              ''', con)
    d=d+1
    indexnames = sql[sql['DaystoClose'] <= d].index
    sql.drop(indexnames, inplace=True)
    sql = sql.reset_index(drop=True)
    unique = sql.question_id.nunique()
    d = d-1
    data = [[d, unique]]
    df = pd.DataFrame(data, columns = ['DaystoClose', 'NumofQuestions'])
    df= df.values.tolist()
    c.executemany('INSERT INTO Count VALUES (?,?)', (df))
    con.commit()   
    d = d+20
    print(d)

