import jqdatasdk as jq
import pandas as pd
from time import sleep
from tqdm import tqdm
from MysqlHelper import MysqlHelper as MySQL
def getAuthJQ():
    jq.auth('13501253295', 'inside')

def getAllStocks():
    getAuthJQ()
    df = jq.get_all_securities(['stock']).index
    return df

def readdata(code,enddate,days:int):
    df=pd.DataFrame()
    getAuthJQ()
    df=jq.get_bars(code, days, unit='1d',
             fields=['date', 'open', 'high', 'low', 'close','volume'],
             end_dt=enddate, df=True,include_now=True)
    df = df.sort_values('date')
    #print(df)
    return df

def createStockBarTable():
    stocks = getAllStocks()
    mysql = MySQL()
    createSql = "DROP TABLE IF EXISTS `mystock`.`stock_bars`; " \
                "CREATE TABLE `stock_bars` (`code` varchar(30) NOT NULL," \
                "`date` datetime NOT NULL," \
                "`open` float DEFAULT NULL," \
                "`high` float DEFAULT NULL," \
                "`low` float DEFAULT NULL," \
                "`close` float DEFAULT NULL," \
                "`volume` bigint DEFAULT NULL," \
                "PRIMARY KEY (`code`,`date`)) " \
                "ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;"
    mysql.connect()
    mysql.exec_sql(createSql)
def setJQDataToStockBars():
   stocks = getAllStocks()
   mysql = MySQL()
   mysql.connect()
   stocks = getAllStocks()
   today = '2021-02-03 15:00:00'
   days = 600
   cnt = 0
   pbar = tqdm(total=len(stocks), ncols=100, unit='B')

   for stock in stocks:
       #print(stock)
       cntSQL = "select count(*) cnt from stock_bars where code = '"+stock+"'"
       count,rows = mysql.select_sql(cntSQL,None)
       cnt+=1
       pbar.set_description('Processing:'+str(cnt))
       sleep(0.2)
       pbar.update(1)
       if (rows[0][0]> 0):
           continue
       stock_df = readdata(stock,today,days)
       for stock_day in stock_df.iterrows():
           code = stock
           date = stock_day[1]["date"].strftime("%Y-%m-%d")
           open = str(stock_day[1]["open"])
           close = str(stock_day[1]["close"])
           high = str(stock_day[1]["high"])
           low = str(stock_day[1]["low"])
           volume = str(stock_day[1]["volume"])

           insertSQL = "INSERT INTO `mystock`.`stock_bars`" \
                       "(`code`,`date`,`open`,`high`,`low`,`close`,`volume`)" \
                       "VALUES('"+code+"','"+date+"',"+open+","+high+","+low+","+close+","+volume+");"
           mysql.exec_sql(insertSQL)
   print("数据导入完成")
   return

setJQDataToStockBars()