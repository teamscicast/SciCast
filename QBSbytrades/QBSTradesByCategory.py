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
             histtrades.trade_id, histtrades.questionid, categories.categoryid
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

c.execute ("DROP TABLE if exists TradesIndexed")
c.execute('''Create table TradesIndexed
              (
              tradeindex INTEGER,
              question_id INTEGER,
              tradeduration INTEGER,
              TBS INTEGER,
              questionduration INTEGER)''')
con.commit()
with open('tradesindexed.csv', 'r') as f:
    reader = csv.reader(f)
    next(reader)
    cats = list(reader)
c.executemany('INSERT INTO TradesIndexed VALUES (?,?,?,?,?)', (cats))
con.commit()  


c.execute ("DROP TABLE if exists BinaryTradeCats")
c.execute('''Create table BinaryTradeCats AS SELECT
             QuestionCats.categoryid,
             TradesIndexed.tradeindex, TradesIndexed.question_id, TradesIndexed.tradeduration,
             TradesIndexed.TBS
             FROM TradesIndexed
             LEFT JOIN QuestionCats
             ON TradesIndexed.question_id = QuestionCats.questionid''')
con.commit()
c.execute ("DELETE FROM BinaryTradeCats WHERE categoryid IS NULL")
con.commit()

c.execute ("DROP TABLE if exists QBSOVERTRADESFORCATS")
c.execute('''Create table QBSOVERTRADESFORCATS
              (
              TradeNumber INTEGER,
              QBS INTEGER,
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
              NumberofTrades INTEGER, 
              Category INTEGER,
              StDev INTEGER,
              SqrtNumRows INTEGER)''')
con.commit()

c.execute ("DROP TABLE if exists QuestionCount")
c.execute('''Create table QuestionCount
              (
              Count INTEGER,
              Category INTEGER,
              TradeNum INTEGER)''')
con.commit()

#Loop for trades
q=1
while q in range(5):
    df=pd.read_csv("BinaryTradeIndex.csv")
    df= df.drop(df[df.categoryid !=q].index)
    df=df.drop(df[df.question_id==747].index)
    df2list= df.values.tolist()
    c.execute ("DROP TABLE if exists TradesIndexedforloop")
    c.execute('''Create table TradesIndexedforloop
              (
              Category INTEGER,
              TradeIndex INTEGER,
              question_id INTEGER,
              difftime_tradedays INTEGER,
              TBS INTEGER)''')
    con.commit()
    c.executemany('INSERT INTO TradesIndexedforloop VALUES (?,?,?,?,?)', (df2list))
    con.commit()
    
    w= q
    print(" q= ", q)
    q=q+1
    r=6
    for x in range(104):  
        sql= pd.read_sql_query('''SELECT Category, TradeIndex, question_id, difftime_tradedays, TBS
            FROM TradesIndexedforloop
              ''', con)
        trades=pd.DataFrame(sql, columns=['Category', 'TradeIndex', 'question_id', 'difftime_tradedays', 'TBS'])
        trades= trades.drop(trades[trades.TradeIndex >=r].index)
        trades= trades.values.tolist()
        c.execute ("DROP TABLE if exists TradesIndexed")
        c.execute('''Create table TradesIndexed
              (
              Category INTEGER,
              TradeIndex INTEGER,
              question_id INTEGER,
              difftime_tradedays INTEGER,
              TBS INTEGER)''')
        con.commit()
        c.executemany('INSERT INTO TradesIndexed VALUES (?,?,?,?,?)', (trades))
        con.commit()  
        c.execute ("DELETE FROM TradesIndexed WHERE question_id==1375")
        con.commit()
        c.execute ("DELETE FROM TradesIndexed WHERE question_id IS NULL")
        con.commit()
        c.execute ("DROP TABLE if exists Temp")
        c.execute('''Create table Temp
              (
              questionid INTEGER,
              questionduration INTEGER,
              Category INTEGER)''')
        con.commit() 
        sql = ('''SELECT question_id, sum(difftime_tradedays), Category
          FROM TradesIndexed
          GROUP BY question_id
             ''')
        con.commit()
        duration = c.execute(sql).fetchall()

        c.executemany('INSERT INTO Temp VALUES (?,?,?)', (duration))
        con.commit()  

        c.execute("DROP TABLE if exists Temp2Join")
        c.execute('''CREATE TABLE Temp2Join as
          SELECT TradesIndexed.TradeIndex, TradesIndexed.question_id, TradesIndexed.difftime_tradedays, TradesIndexed.TBS, Temp.questionduration, Temp.Category
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

        sql= ('''Select max(TradeIndex), question_id, sum(QBSindivid), Category
            FROM Temp2Join
            group by question_id
              ''')
        togroupbyquestion = c.execute(sql).fetchall()
        c.execute ("DROP TABLE if exists questiongrouping")
        c.execute('''Create table questiongrouping
              (
              tradenum INTEGER,
              questionid INTEGER,
              QBS INTEGER,
              Category INTEGER)''')
        con.commit()
        c.executemany('INSERT INTO questiongrouping VALUES (?,?,?,?)', (togroupbyquestion))
        con.commit() 
        r=r-4
        sql= pd.read_sql_query('''SELECT tradenum, questionid, Category, QBS
            FROM questiongrouping
              ''', con)
        grouping=pd.DataFrame(sql, columns=['tradenum', 'questionid', 'Category', 'QBS'])
        indexnames = grouping[grouping['tradenum'] <= r].index
        grouping.drop(indexnames, inplace=True)
        c.execute ("DROP TABLE if exists questiongrouping")
        c.execute('''Create table questiongrouping
              (
              tradenum INTEGER,
              questionid INTEGER,
              Category INTEGER,
              QBS INTEGER)''')
        con.commit()
        grouping= grouping.values.tolist()
        c.executemany('INSERT INTO questiongrouping VALUES (?,?,?,?)', (grouping))
        con.commit()
#Question Grouping is table with valid QBS Scores for specific loop  
        sql= ('''Select max(tradenum), avg(QBS), Category
            FROM questiongrouping
              ''')
        average = c.execute(sql).fetchall()
        c.executemany('INSERT INTO QBSOVERTRADESFORCATS VALUES (?,?,?)', (average))
        con.commit()      
        c.execute ("DROP TABLE if exists TradesIndexed")
        c.execute('''Create table TradesIndexed
              (
              Category INTEGER,
              TradeIndex INTEGER,
              question_id INTEGER,
              difftime_tradedays INTEGER,
              TBS INTEGER)''')
        con.commit()
            #Getting count questions at each day group for categories
        sql= ('''Select count(questionid), max(Category), max(tradenum)
            FROM questiongrouping
              ''')
        counting = c.execute(sql).fetchall()
        c.executemany('INSERT INTO QuestionCount VALUES (?,?,?)', (counting))
        con.commit() 

    

#CALCULATE STANDARD DEVIATION OF QBS AT EACH TRADE
    #Calculate mean
        sql= pd.read_sql_query('''SELECT TradeNumber, QBS, Category
            FROM QBSOVERTRADESFORCATS
              ''', con)
        formean=pd.DataFrame(sql, columns=['TradeNumber', 'QBS', 'Category'])
        r=r+3
        indexnames = formean[formean['TradeNumber'] != r].index
        formean.drop(indexnames, inplace=True)
        formean = formean.reset_index(drop=True)
        indexnames = formean[formean['Category'] != w].index
        formean.drop(indexnames, inplace=True)
        formean = formean.reset_index(drop=True)
        length = len(formean)
        if length != 0:
            formean['QBS'] = formean['QBS'].astype(float)
            mean = formean.at[0, 'QBS']
    #Subtract each number from mean and square result
            sql= pd.read_sql_query('''SELECT tradenum, questionid, Category, QBS
            FROM questiongrouping
              ''', con)
            insertingdata=pd.DataFrame(sql, columns=['tradenum', 'questionid', 'Category','QBS'])
            insertingdata.insert(4, "AvgQBS", mean) 
            insertingdata= insertingdata.values.tolist()
            c.execute ("DROP TABLE if exists squareddiffs")
            c.execute('''Create table squareddiffs
                      (
              tradenum INTEGER,
              questionid INTEGER,
              Category INTEGER,
              QBS INTEGER,
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
            sql= pd.read_sql_query('''Select tradenum, Category, squareddiff
            FROM squareddiffs
              ''', con)
            numrows = sql.shape[0]
            tradenums = sql['tradenum'].max()
            Category = sql['Category'].max()
            total= sql['squareddiff'].sum()
            if numrows >=1:
                forsd = total/numrows
                sqrtnumrows = np.sqrt(numrows)
                forsd= np.sqrt(forsd)
                data = [[tradenums, Category, forsd, sqrtnumrows]]
                df = pd.DataFrame(data, columns = ['TradeNum', 'Category', 'StDev', 'SqrtNumRows'])
                df= df.values.tolist()
                c.executemany('INSERT INTO stdevs VALUES (?,?,?,?)', (df))
                con.commit()   
        r=r+6
        print(r)


c.execute ("DELETE FROM QBSOVERTRADESFORCATS WHERE TradeNumber IS NULL")
con.commit()

c.execute ("DELETE FROM stdevs WHERE StDev =0")
con.commit()

#Conf Intervals
c.execute ("DROP TABLE if exists ConfIntervalsforQBSOverTradeByCat")
c.execute('''CREATE TABLE ConfIntervalsforQBSOverTradeByCat as
          SELECT stdevs.NumberofTrades, QBSOVERTRADESFORCATS.QBS, stdevs.Category, stdevs.StDev, stdevs.SqrtNumRows
          FROM QBSOVERTRADESFORCATS
          LEFT JOIN stdevs
          ON QBSOVERTRADESFORCATS.TradeNumber=stdevs.NumberofTrades
          AND QBSOVERTRADESFORCATS.Category=stdevs.Category''')
con.commit()

c.execute('''ALTER TABLE ConfIntervalsforQBSOverTradeByCat
            ADD SdoverSqrtNum INTEGER
              ''')
con.commit()
c.execute("UPDATE ConfIntervalsforQBSOverTradeByCat set SdoverSqrtNum= StDev/SqrtNumRows ")
con.commit()

c.execute('''ALTER TABLE ConfIntervalsforQBSOverTradeByCat
            ADD TimesZ INTEGER
              ''')
con.commit()
c.execute("UPDATE ConfIntervalsforQBSOverTradeByCat set TimesZ= SdoverSqrtNum*1.96 ")
con.commit()

c.execute('''ALTER TABLE ConfIntervalsforQBSOverTradeByCat
            ADD UpperBound INTEGER
              ''')
con.commit()
c.execute("UPDATE ConfIntervalsforQBSOverTradeByCat set UpperBound= QBS + TimesZ ")
con.commit()

c.execute('''ALTER TABLE ConfIntervalsforQBSOverTradeByCat
            ADD LowerBound INTEGER
              ''')
con.commit()
c.execute("UPDATE ConfIntervalsforQBSOverTradeByCat set LowerBound= QBS - TimesZ ")
con.commit()

c.execute ("DELETE FROM ConfIntervalsforQBSOverTradeByCat WHERE NumberofTrades IS NULL")
con.commit()

c.execute ("DELETE FROM ConfIntervalsforQBSOverTradeByCat WHERE LowerBound <=0 ")
con.commit()


#The final QBS over trade by category data can be found in table "QBSOVERTRADESFORCATS"
