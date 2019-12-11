import csv
import sqlite3
import pandas as pd
import numpy as np

con = sqlite3.connect('SciCast.db')
c = con.cursor()


c.execute ("DROP TABLE if exists histtrades")
c.execute('''Create table histtrades
              (
              Trade_id INTEGER,
              questionid INTEGER)''')

with open('historical_trades.csv', 'r') as f:
    reader = csv.reader(f)
    next(reader)
    trades = list(reader)
c.executemany('INSERT INTO histtrades VALUES (?,?)', (trades))
con.commit()  


c.execute ("DROP TABLE if exists categories")
c.execute('''Create table categories
              (
              questionid INTEGER,
              categoryid INTEGER)''')

with open('question_category_links.csv', 'r') as f:
    reader = csv.reader(f)
    next(reader)
    cats = list(reader)
c.executemany('INSERT INTO categories VALUES (?,?)', (cats))
con.commit()  

c.execute ("DROP TABLE if exists joined")
c.execute('''Create table joined AS SELECT
             histtrades.trade_id, categories.questionid, categories.categoryid
             FROM histtrades
             LEFT JOIN categories
              ON histtrades.questionid = categories.questionid''')
con.commit()

c.execute ("DROP TABLE if exists BINARYTrades")
c.execute('''Create table BINARYTrades
              (
              question_id INTEGER,
              trade_id INTEGER,
              Ser STRING,
              NVLINTEGER)''')
con.commit()
with open('BinaryTrades.csv', 'r') as f:
    reader = csv.reader(f)
    next(reader)
    cats = list(reader)
c.executemany('INSERT INTO BINARYTrades VALUES (?,?,?,?)', (cats))
con.commit()  

c.execute ("DROP TABLE if exists binarytradescats")
c.execute('''Create table binarytradescats AS SELECT
             BINARYTrades.trade_id, joined.questionid, joined.categoryid
             FROM BINARYTrades
             LEFT JOIN joined
              ON BINARYTrades.Trade_id = joined.Trade_id''')
con.commit()
c.execute ("DELETE FROM binarytradescats WHERE questionid IS NULL")
con.commit()
c.execute ("DELETE FROM binarytradescats WHERE categoryid == 14")
con.commit()
c.execute ("DELETE FROM binarytradescats WHERE categoryid == 15")
con.commit()
c.execute ("DELETE FROM binarytradescats WHERE categoryid == 16")
con.commit()
c.execute ("DELETE FROM binarytradescats WHERE categoryid == 19")
con.commit()
c.execute ("DELETE FROM binarytradescats WHERE categoryid == 20")
con.commit()
c.execute ("DELETE FROM binarytradescats WHERE categoryid == 21")
con.commit()
c.execute ("DELETE FROM binarytradescats WHERE categoryid == 18")
con.commit()
c.execute ("DELETE FROM binarytradescats WHERE categoryid == 22")
con.commit()
c.execute ("DELETE FROM binarytradescats WHERE categoryid == 11")
con.commit()
c.execute ("DELETE FROM binarytradescats WHERE categoryid == 17")
con.commit()
#Category 1 = Earth Studies
#Category 2 = Hard Sciences
#Category 3 = Business of Science
#Category 4 = Engineered Technologies
c.execute("UPDATE binarytradescats set categoryid== 2 WHERE categoryid==3 ")
con.commit()
c.execute("UPDATE binarytradescats set categoryid== 4 WHERE categoryid==5 ")
con.commit()
c.execute("UPDATE binarytradescats set categoryid== 4 WHERE categoryid==8 ")
con.commit()
c.execute("UPDATE binarytradescats set categoryid== 1 WHERE categoryid==6 ")
con.commit()
c.execute("UPDATE binarytradescats set categoryid== 1 WHERE categoryid==10 ")
con.commit()
c.execute("UPDATE binarytradescats set categoryid== 2 WHERE categoryid==9 ")
con.commit()
c.execute("UPDATE binarytradescats set categoryid== 2 WHERE categoryid==7 ")
con.commit()
c.execute("UPDATE binarytradescats set categoryid== 2 WHERE categoryid==12 ")
con.commit()
c.execute("UPDATE binarytradescats set categoryid== 3 WHERE categoryid==13 ")
con.commit()

sql = ('''SELECT DISTINCT questionid, categoryid
    FROM binarytradescats
    GROUP BY questionid''')
trades = c.execute(sql).fetchall()
c.execute ("DROP TABLE if exists QuestionCats")
c.execute('''Create table QuestionCats
              (
              questionid INTEGER,
              categoryid INTEGER)''')
con.commit()
c.executemany('INSERT INTO QuestionCats VALUES (?,?)', (trades))
con.commit()


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
            ADD DaystoClose INTEGER
              ''')
con.commit()

c.execute("UPDATE dayandquestsum set DaystoClose= (questionduration - SumofPrevious) ")
con.commit()

c.execute ("DROP TABLE if exists DaysSumCat")
c.execute('''CREATE TABLE DaysSumCat as
          SELECT dayandquestsum.question_id, dayandquestsum.TBS, dayandquestsum.QBS, dayandquestsum.tradeduration, dayandquestsum.SumofPrevious, dayandquestsum.questionduration, dayandquestsum.DaystoClose, QuestionCats.categoryid 
          FROM dayandquestsum
          LEFT JOIN QuestionCats
          ON dayandquestsum.question_id=QuestionCats.questionid''')
con.commit()

c.execute ("DROP TABLE if exists QBSOVERDAYSFORCATS")
c.execute('''Create table QBSOVERDAYSFORCATS
              (
              DaysTilClose INTEGER,
              AverageQBS INTEGER,
              Category INTEGER)''')
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
              Days INTEGER,
              Category INTEGER,
              StDev INTEGER,
              SqrtNumRows INTEGER)''')
con.commit()

c.execute ("DROP TABLE if exists QuestionCount")
c.execute('''Create table QuestionCount
              (
              Count INTEGER,
              Category INTEGER,
              DaysTilClose INTEGER)''')
con.commit()

c.execute ("DROP TABLE if exists AvgTradesPerQ")
c.execute('''Create table AvgTradesPerQ
              (
              NumTrades INTEGER,
              NumQuestions INTEGER,
              DaysTilClose INTEGER,
              Category INTEGER)''')
con.commit()

#Loop for rest of trade
q=1
while q in range(5):
    df=pd.read_csv("DaysSumCat.csv")
    df= df.drop(df[df.categoryid !=q].index)
    df2list= df.values.tolist()
    c.execute ("DROP TABLE if exists Tradesforloop")
    c.execute('''Create table Tradesforloop
              (
              question_id INTEGER,
              TBS INTEGER,
              QBS INTEGER,
              tradeduration INTEGER,
              SumofPrevious INTEGER,
              questionduration INTEGER,
              DaystoClose INTEGER,
              categoryid INTEGER)''')
    con.commit()
    c.executemany('INSERT INTO Tradesforloop VALUES (?,?,?,?,?,?,?,?)', (df2list))
    con.commit()
    c.execute ("DELETE FROM Tradesforloop WHERE categoryid IS NULL")
    con.commit()  
    w= q
    print(" q= ", q)
    q=q+1
    r=5
    for x in range(50):  
        sql= pd.read_sql_query('''SELECT question_id, TBS, QBS, tradeduration, SumofPrevious, questionduration, DaystoClose, categoryid
            FROM Tradesforloop
              ''', con)
        trades=pd.DataFrame(sql, columns=['question_id', 'TBS', 'QBS', 'tradeduration', 'SumofPrevious', 'questionduration', 'DaystoClose', 'categoryid'])
        trades= trades.drop(trades[trades.DaystoClose < r].index)
        trades2=trades
        trades= trades.values.tolist()
        c.execute ("DROP TABLE if exists Trades")
        c.execute('''Create table Trades
              (
              question_id INTEGER,
              TBS INTEGER,
              QBS INTEGER,
              tradeduration INTEGER,
              SumofPrevious INTEGER,
              questionduration INTEGER,
              DaystoClose INTEGER,
              categoryid INTEGER)''')
        con.commit()
        c.executemany('INSERT INTO Trades VALUES (?,?,?,?,?,?,?,?)', (trades))
        con.commit()  
        c.execute ("DELETE FROM Trades WHERE question_id IS NULL")
        con.commit()
        c.execute ("DROP TABLE if exists Temp")
        c.execute('''Create table Temp
              (
              questionid INTEGER,
              questiondurationfordays INTEGER)''')
        con.commit()
         
        sql = ('''SELECT question_id, sum(tradeduration)
          FROM Trades
          GROUP BY question_id
             ''')
        con.commit()
        duration = c.execute(sql).fetchall()
        
        c.executemany('INSERT INTO Temp VALUES (?,?)', (duration))
        con.commit()  

        c.execute("DROP TABLE if exists Temp2Join")
        c.execute('''CREATE TABLE Temp2Join as
          SELECT Trades.question_id, Trades.tradeduration, Trades.SumofPrevious,Trades.TBS, Trades.Daystoclose, Trades.categoryid, Temp.questiondurationfordays
          FROM Trades
          LEFT JOIN Temp
          ON Trades.question_id=Temp.questionid''')
        con.commit()
        c.execute('''ALTER TABLE Temp2Join
            ADD QBSindivid INTEGER
              ''')
        con.commit()

        c.execute("UPDATE Temp2Join set QBSindivid= (TBS * (tradeduration/questiondurationfordays)) ")
        con.commit()
        
        sql= ('''Select min(Daystoclose), question_id, sum(QBSindivid), categoryid
            FROM Temp2Join
            group by question_id
              ''')
        togroupbyquestion = c.execute(sql).fetchall()
        c.execute ("DROP TABLE if exists questiongrouping")
        c.execute('''Create table questiongrouping
              (
              DaysTilClose INTEGER,
              questionid INTEGER,
              QBS INTEGER,
              Category INTEGER)''')
        con.commit()
        c.executemany('INSERT INTO questiongrouping VALUES (?,?,?,?)', (togroupbyquestion))
        con.commit() 
        #Average num of trades on question by days
        tradesforavg = len(trades2)
        questionsforavg = len(trades2['question_id'].drop_duplicates())
        q = q-1
        data = [[tradesforavg, questionsforavg, r, q]]
        q= q+1
        df = pd.DataFrame(data, columns = ['TotalTrades', 'TotalQuestions', 'DaysTilClose', 'Category'])
        df= df.values.tolist()
        c.executemany('INSERT INTO AvgTradesPerQ VALUES (?,?,?,?)', (df))
        con.commit()         
        #Question Grouping is table with valid QBS Scores for specific loop   
        sql= ('''Select min(DaysTilClose), avg(QBS), min(Category)
            FROM questiongrouping
              ''')
        average = c.execute(sql).fetchall()
        average=pd.DataFrame(average)
        average.at[0,0] = r
        average= average.values.tolist()
        c.executemany('INSERT INTO QBSOVERDAYSFORCATS VALUES (?,?,?)', (average))
        con.commit() 
        
        #Getting count questions at each day group for categories
        sql= ('''Select count(questionid), max(Category), min(DaysTilClose)
            FROM questiongrouping
              ''')
        counting = c.execute(sql).fetchall()
        c.executemany('INSERT INTO QuestionCount VALUES (?,?,?)', (counting))
        con.commit() 
        c.execute("UPDATE QuestionCount set DaysTilClose= ROUND(DaysTilClose)")
        con.commit()
#CALCULATE STANDARD DEVIATION OF QBS AT EACH TRADE
    #Calculate mean
        sql= pd.read_sql_query('''SELECT DaysTilClose, AverageQBS, Category
            FROM QBSOVERDAYSFORCATS
              ''', con)
        formean=pd.DataFrame(sql, columns=['DaysTilClose', 'AverageQBS', 'Category'])
        indexnames = formean[formean['DaysTilClose'] != r].index
        formean.drop(indexnames, inplace=True)
        formean = formean.reset_index(drop=True)
        indexnames = formean[formean['Category'] != w].index
        formean.drop(indexnames, inplace=True)
        formean = formean.reset_index(drop=True)
        length = len(formean)
        if length != 0:
            formean['AverageQBS'] = formean['AverageQBS'].astype(float)
            mean = formean.at[0, 'AverageQBS']
    #Subtract each number from mean and square result
            sql= pd.read_sql_query('''SELECT DaysTilClose, questionid, QBS, Category
            FROM questiongrouping
              ''', con)
            insertingdata=pd.DataFrame(sql, columns=['DaysTilClose', 'questionid', 'QBS', 'Category'])
            insertingdata.insert(4, "AvgQBS", mean) 
            insertingdata= insertingdata.values.tolist()
            c.execute ("DROP TABLE if exists squareddiffs")
            c.execute('''Create table squareddiffs
                      (
              DaysTilClose INTEGER,
              questionid INTEGER,
              QBS INTEGER,
              Category INTEGER,
              AvgQBS INTEGER)''')
            con.commit()
            c.executemany('INSERT INTO squareddiffs VALUES (?,?,?,?,?)', (insertingdata))
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
            sql= pd.read_sql_query('''Select DaysTilClose, Category, squareddiff
            FROM squareddiffs
              ''', con)
            numrows = sql.shape[0]
            sql['DaysTilClose'].round(0)
            days = sql['DaysTilClose'].min()    
            Category = sql['Category'].max()
            total= sql['squareddiff'].sum()
            forsd = total/numrows
            sqrtnumrows = np.sqrt(numrows)
            forsd= np.sqrt(forsd)
            data = [[days, Category, forsd, sqrtnumrows]]
            df = pd.DataFrame(data, columns = ['DaysTilClose', 'Category', 'StDev', 'SqrtNumRows'])
            df= df.values.tolist()
            c.executemany('INSERT INTO stdevs VALUES (?,?,?,?)', (df))
            con.commit()   
        r=r+5
        print(r)

c.execute("UPDATE stdevs set Days=Round(Days,0) ")
con.commit()

#Fixing Avg Num Trades Table
c.execute('''ALTER TABLE AvgTradesPerQ
            ADD AvgNumofTradesatDays INTEGER
              ''')
con.commit() 
c.execute("UPDATE AvgTradesPerQ set AvgNumofTradesatDays= NumTrades/NumQuestions ")
con.commit()
  

#Conf Intervals
c.execute ("DROP TABLE if exists ConfIntervalsforQBSOverDaysByCat")
c.execute('''CREATE TABLE ConfIntervalsforQBSOverDaysByCat as
          SELECT stdevs.Days, QBSOVERDAYSFORCATS.AverageQBS, stdevs.Category, stdevs.StDev, stdevs.SqrtNumRows
          FROM QBSOVERDAYSFORCATS
          LEFT JOIN stdevs
          ON QBSOVERDAYSFORCATS.DaysTilClose=stdevs.Days
          AND QBSOVERDAYSFORCATS.Category=stdevs.Category''')
con.commit()

c.execute('''ALTER TABLE ConfIntervalsforQBSOverDaysByCat
            ADD SdoverSqrtNum INTEGER
              ''')
con.commit()
c.execute("UPDATE ConfIntervalsforQBSOverDaysByCat set SdoverSqrtNum= StDev/SqrtNumRows ")
con.commit()

c.execute('''ALTER TABLE ConfIntervalsforQBSOverDaysByCat
            ADD TimesZ INTEGER
              ''')
con.commit()
c.execute("UPDATE ConfIntervalsforQBSOverDaysByCat set TimesZ= SdoverSqrtNum*1.96 ")
con.commit()

c.execute('''ALTER TABLE ConfIntervalsforQBSOverDaysByCat
            ADD UpperBound INTEGER
              ''')
con.commit()
c.execute("UPDATE ConfIntervalsforQBSOverDaysByCat set UpperBound= AverageQBS + TimesZ ")
con.commit()

c.execute('''ALTER TABLE ConfIntervalsforQBSOverDaysByCat
            ADD LowerBound INTEGER
              ''')
con.commit()
c.execute("UPDATE ConfIntervalsforQBSOverDaysByCat set LowerBound= AverageQBS - TimesZ ")
con.commit()

c.execute ("DELETE FROM ConfIntervalsforQBSOverDaysByCat WHERE Days IS NULL")
con.commit()

c.execute ("DELETE FROM ConfIntervalsforQBSOverDaysByCat WHERE LowerBound <=0 ")
con.commit()
#The final QBS over days by category data can be found in table "QBSOVERDAYSFORCATS"
