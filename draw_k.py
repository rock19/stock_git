import jqdatasdk as jq
import pandas as pd
from matplotlib import pyplot as plt
import mplfinance as mpf
import numpy as np
import datetime
from MysqlHelper import MysqlHelper as MySQL
from time import sleep
from tqdm import tqdm

def getAuthJQ():
    jq.auth('13501253295', 'inside')

def readdata(code,today,days:int):
    s = datetime.datetime.strptime(today, '%Y-%m-%d').date() - datetime.timedelta(days=days)
    endday = s.strftime("%Y-%m-%d")
    stockSQL = "SELECT code,date,open,high,low,close,volume FROM mystock.stock_bars_memory " \
               "where date between '" + endday + "' and '" + today + "' and code ='" + code + "'"
    # print(stockSQL)
    mysql = MySQL()
    mysql.connect()
    cnt, rows = mysql.select_sql(stockSQL, None)
    columns = {'code': 1, 'date': 2, 'open': 3, 'high': 4, 'low': 5, 'close': 6, 'volume': 7}
    df = pd.DataFrame(rows, columns=columns)
    df = df.sort_values('date')
    #print(df)
    return df

def getAllStocks():
    #getAuthJQ()
    codesql = "select distinct code from mystock.stock_bars_memory;"
    mysql = MySQL()
    mysql.connect()
    cnt, rows = mysql.select_sql(codesql, None)
    columns = {'code'}
    df = pd.DataFrame(rows, columns=columns)
    #df = jq.get_all_securities(['stock']).index
    return df

def dataProcess(today:str,days:int) -> (np.array, np.array):
   stocks = getAllStocks()
   pbar = tqdm(total=len(stocks), ncols=100, unit='B')
   cnt = 0
   for stock in stocks["code"]:
       #print(stock)
       check_stock(stock,today,days)
       cnt += 1
       pbar.set_description('Processing:' + str(cnt) + ",code = " + stock)
       pbar.update(1)
   return

def check_stock(codestr:str,today:str,days:int):
    #s = datetime.datetime.strptime(today,'%Y-%m-%d').date() - datetime.timedelta(days = days)
    s = datetime.datetime.strptime(today,'%Y-%m-%d').date() - datetime.timedelta(days = days)
    endday = s.strftime("%Y-%m-%d")
    stockSQL = "SELECT code,date,open,high,low,close,volume FROM mystock.stock_bars_memory " \
               "where date between '"+endday+"' and '"+ today +"' and code ='"+codestr+"'"
    #print(stockSQL)
    mysql = MySQL()
    mysql.connect()
    cnt,rows =mysql.select_sql(stockSQL,None)
    columns = {'code':1, 'date':2, 'open':3, 'high':4, 'low':5, 'close':6, 'volume':7}
    df=pd.DataFrame(rows,columns=columns)
    rows_list=[]
    row = 0

    rows_dict = {}
    for rowdata in df.iterrows():
        open_price = rowdata[1]['open']
        close_price = rowdata[1]['close']
        high_price = rowdata[1]['high']
        low_price = rowdata[1]['low']
        volume = rowdata[1]['volume']
        #end_date = rowdata[1]["date"].strftime('%Y-%m-%d') + " 15:00:00"
        end_date = rowdata[1]["date"]
        #df_T = readdata(code, end_date, 30)
        if (row < 30):
            row += 1
        else :
            df_T = df.loc[row-29:row]
            open_yesterday = df_T['close'][row -1]
            close_yesterday = df_T['open'][row -1]
            volume_yesterday = df_T['volume'][row -1]
            T_sharp = check_t_jq(open_price, close_price, high_price, low_price, volume,open_yesterday,close_yesterday,volume_yesterday)
            mean_volume = df_T['volume'][row -5:row].mean()
            # 实体大小
            if  T_sharp and df_T['open'][row] < df_T['close'][row-1] and mean_volume*1.3 < volume:
                rows_dict.update(rowdata[1])
                rows_list.append(rows_dict)
            row += 1
    if len(rows_list) != 0 :
        draw_k(df, rows_list)
    #draw_k(df, rows_list)
    return

def check_t_jq(open,close,high,low,volume,open_yesterday,close_yesterday,volume_yesterday):
    if (close - open) >= 0:   #定义柱体的上边界和下边界
        top = close
        botton = open
    else:
        top = open
        botton = close
    if (close_yesterday-open_yesterday)>=0:
        top_yesterday = close_yesterday
        botton_yesterday = open_yesterday
    else :
        top_yesterday = close_yesterday
        botton_yesterday = open_yesterday
    cylinder_ = close * 0.02 #预设实体大小
    cylinder = top - botton #实际实体大小
    radio_low = 2*cylinder #下线4倍实体大小
    radio_cylinder= 0.02

    if (cylinder < cylinder_) and (cylinder > cylinder_*0.3) and ((high - top) < cylinder_*0.2): #上影线不超过预设实体大小的20%
        if botton - low > radio_low :
            # if botton_yesterday > open and volume > volume_yesterday: #今日低开且成交量放大
            #     return True #找到
            return True  # 找到
    return False

def get_maList(df:pd.DataFrame,n:int):
    mean = df[:n]['close'].mean()
    return mean

def draw_k(df:pd.DataFrame,mark):

    #df['date'] = datetime.datetime.strptime(df['date'],'%Y-%m-%d').date()
        #str(df['date']).apply(lambda x: datetime.strptime(x, '%Y%m%d'))
    data = df.loc[:, ['date', 'open', 'close', 'high', 'low', 'volume']]
    data.columns = ['Date', 'Open','Close','High','Low','Volume']
    data.set_index('Date', inplace=True)
    data.index.name = "Date"
    data.index = pd.DatetimeIndex(data.index)
    data.shape
    mark_df = pd.DataFrame(columns = ['Date','low'])
    mark_df.set_index('Date', inplace=True)
    mark_df.index.name = "Date"
    mark_df.shape
    mc = mpf.make_marketcolors(up='r', down='g')
    s = mpf.make_mpf_style(marketcolors=mc, mavcolors=['#4f8a8b', '#fbd46d', '#87556f'])
    #add_plot = mpf.make_addplot(data['low'])
    if len(mark)>0:
        for row in  df.iterrows():
            flag = False
            for s in mark:
                # if row[1]["date"].strftime('%Y-%m-%d')=='2020-10-15':
                #     a=1
                if s["date"] == row[1]["date"]:
                    flag = True
                    break
            if flag :
                new = pd.DataFrame({"low": row[1]["low"]}, index=[row[1]["date"]])
                mark_df = mark_df.append(new)
            else:
                new = pd.DataFrame({"low":np.nan}, index=[row[1]["date"]])
                mark_df=mark_df.append(new)

        add_plot = [
            mpf.make_addplot(mark_df, scatter=True, markersize=50, marker='^', color='r')
            ]
        # mpf.plot(data, addplot=add_plot, type='candle', volume=True, mav=(7, 13, 26), show_nontrading=True,
        #           figratio=(20, 10))
        # ('candle', 'candlestick', 'ohlc', 'ohlc_bars',
        #  'line', 'renko', 'pnf')
        plt.savefig('fig.png', bbox_inches='tight')
        mpf.plot(data, type='candle', addplot=add_plot, volume=True,ylabel='price', ylabel_lower='volume')
    else:
        mpf.plot(data, type='candle', volume=True, mav=(7, 13, 26), show_nontrading=True,
                 datetime_format='%Y-%m-%d', figratio=(50, 30))



# getAuthJQ
code = '000876.XSHE'
today='2021-02-01'

dataProcess(today,300)

