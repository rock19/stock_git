import jqdatasdk as jq
import pandas as pd
from matplotlib import pyplot as plt
import mplfinance as mpf
import numpy as np
import datetime


def readdata(code,enddate,days:int):
    df=pd.DataFrame()
    jq.auth('13501253295', 'inside')
    df=jq.get_bars(code, days, unit='1d',
             fields=['date', 'open', 'high', 'low', 'close','volume'],
             end_dt=enddate, df=True,include_now=True)
    df = df.sort_values('date')
    #print(df)
    return df

def dataProcess(stock,today,yesterday,days) -> (np.array, np.array):
    todaybar = readdata(stock,today,1)
    historybar = readdata(stock,yesterday,days)

    return

def check_T(code,today):
    df=readdata(code,today,300)
    rows_list=[]
    row = 0
    for rowdata in df.iterrows():
        open_price = rowdata[1]['open']
        close_price = rowdata[1]['close']
        high_price = rowdata[1]['high']
        low_price = rowdata[1]['low']
        if (close_price - open_price) >= 0:
            top_price = close_price
            botton_price = open_price
        else:
            top_price = open_price
            botton_price = close_price
        cylinder = top_price - botton_price  # 实体大小
        if (cylinder / close_price < 0.02) and ((high_price - top_price) < cylinder):
            if botton_price - low_price > 4 * cylinder:
                T_sharp = True
                end_date = rowdata[1]["date"].strftime('%Y-%m-%d')+" 15:00:00"
                df_T = readdata(code,datetime.datetime.strptime(end_date, "%Y-%m-%d %H:%M:%S"),15)
                strs = rowdata[1]["date"].strftime('%Y-%m-%d') + ",low_price=" + str(low_price) + ",high_price" + str(high_price) + ",top_price" \
                       + str(top_price) + ",botton_price" + str(botton_price)
                print(strs)
                rows_dict = {}
                rows_dict.update(rowdata[1])
                rows_list.append(rows_dict)
                draw_k(df_T, rows_list)
    #draw_k(df, rows_list)
    return

def draw_k(df:pd.DataFrame,mark):
    #df['date'] = str(df['date']).apply(lambda x: datetime.strptime(x, '%Y%m%d'))
    data = df.loc[:, ['date', 'open', 'close', 'high', 'low', 'volume']]
    data.set_index('date', inplace=True)
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
                if row[1]["date"].strftime('%Y-%m-%d')=='2019-12-19':
                    a=1
                if s["date"].strftime('%Y-%m-%d') == row[1]["date"].strftime('%Y-%m-%d'):
                    flag = True
                    break
            if flag :
                new = pd.DataFrame({"low": row[1]["low"]}, index=[row[1]["date"]])
                mark_df = mark_df.append(new)
            else:
                new = pd.DataFrame({"low":np.nan}, index=[row[1]["date"]])
                mark_df=mark_df.append(new)

        add_plot = [
            mpf.make_addplot(mark_df, scatter=True, markersize=200, marker='^', color='r')
            ]
        # mpf.plot(data, addplot=add_plot, type='candle', volume=True, mav=(7, 13, 26), show_nontrading=True,
        #           figratio=(20, 10))
        mpf.plot(data, type='candle', addplot=add_plot, volume=True)
    else:
        mpf.plot(data, type='candle', volume=True, mav=(7, 13, 26), show_nontrading=True,
                 datetime_format='%Y-%m-%d', figratio=(20, 10))

    # add_plot = [
    #     mpf.make_addplot(b_list, scatter=True, markersize=200, marker='^', color='y'),
    #     mpf.make_addplot(s_list, scatter=True, markersize=200, marker='v', color='r'),
    #     mpf.make_addplot(data[['UpperB', 'LowerB']])]


code = '000876.XSHE'
today='2021-01-29 15:00:00'
df = check_T(code,today)
#draw_k(df)
# df = df.rename(columns={"vol": "volume"})
# df.set_index('date', inplace=True)
# df.index.name = "Date"
# df.index = pd.DatetimeIndex(df.index)
# df.shape
# mc = mpf.make_marketcolors(up='r',down='g')
# s  = mpf.make_mpf_style(marketcolors=mc,mavcolors=['#4f8a8b','#fbd46d','#87556f'])
# mpf.plot(df,type='candle',volume=True, show_nontrading=True, figratio=(20,10), mav=(5,10,15),style=s)
# fig = plt.figure(figsize=(8, 6), dpi=72, facecolor="white")
# axes = plt.subplot(111)
# axes.set_title('Shangzheng')
# axes.set_xlabel('time')
# line, = axes.plot([], [], linewidth=1.5, linestyle='-')
# plt.plot(range(df.shape[0]),(df['close']))
# plt.plot(range(df.shape[0]),(df['high']))
# plt.plot(range(df.shape[0]),(df['low']))
# plt.xticks(range(0,df.shape[0],900),df['date'].loc[::900],rotation=45)
# plt.xlabel('Date',fontsize=18)
# plt.ylabel('Mid Price',fontsize=18)

#plt.show()