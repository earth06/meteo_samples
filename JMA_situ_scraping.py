import os
import datetime
import csv
import urllib.request
from bs4 import BeautifulSoup
import pandas as pd
import numpy as np
from time import sleep
import glob

"""
気象庁の地上観測データを取得する関数群



ex) 日データを取得
daterange=pd.date_range("2000-01-01","2021-10-01",freq="MS")
count=0
for prec_no,block_no in zip(dfjapan[dfjapan["IndexNbr"]>last_block_no]["prec_no"], dfjapan[dfjapan["IndexNbr"]>last_block_no]["IndexNbr"]):
    dflist=[]
    for datetime in daterange:
        dflist.append(get_daily_obs(prec_no,block_no, datetime))
    dfall=pd.concat(dflist)
    dfall.to_csv(f"./daily/situ_obs_daily_{block_no}_202001_202110.csv",index=None)
    del dfall, dflist
    print(block_no," end")

ex) 気温の月平均値を取得
#dflist=[]
for prec_no,block_no in zip(dfjapan["prec_no"], dfjapan["IndexNbr"]):
    print(block_no)
    dflist.append(get_monthly_obs(prec_no, block_no,var="precip"))

ex) 時間別平均値を取得
dflist=[]
for date in pd.date_range("2015-01-01 00:00","2020-12-31 00:00",freq="D"):
    dftmp=get_hourly_obs(51,47636, date)
    dflist.append(dftmp)
    del dftmp
    print(date)
"""

##################
## hourly       ##
##################
columns_hourly=["datetime","P","P0","precip","temperature","dew_point","Pv","RH","wind_speed","wind_direction",
               "sunlight_time","solar_rad_flux","snow_fall","snow_cover"]
def get_hourly_obs(prec_no, block_no,datetime, debug=False):
    year=datetime.year
    month=datetime.month
    day=datetime.day
    trans=str.maketrans({")":"", "]":"","×":"NaN"})
    url=f"https://www.data.jma.go.jp/obd/stats/etrn/view/hourly_s1.php?prec_no={prec_no}&block_no={block_no}&year={year}&month={month}&day={day}&view="
    html=urllib.request.urlopen(url).read()
    soup=BeautifulSoup(html)
    trs=soup.find("table",{"class":"data2_s"})
    dfall=pd.DataFrame(columns=columns_hourly)
    for idx,tr in enumerate(trs.find_all("tr")[2:]):
        
        tds=tr.find_all("td")
        #デコード
        hour=int(tds[0].string)
        param_list=[]
        param_list.append(datetime+pd.offsets.Hour(hour))
        for i in range(1,13+1):
            param_list.append(tds[i].string)
        dfall.loc[idx,columns_hourly]=param_list
    return dfall

#########################
##  daily
########################
columns=[
    "days",#0
    "Ps_mean", #1
    "P0_mean", #2
    #"P0_min", #3
    #"P0_min_time", #4
    "precip_total", #5
    #"precip_1h_max", #6
    #"precip_1h_max_time", #7
    #"precip_10min_max", #8
    #"precip_10min_max_time", #9
    "temp_mean", # 10
    "temp_max", # 11
    #"temp_max_time", #12
    "temp_min", #13
    #"temp_min_time", #14
    #"P_vaper", #15
    #"RH_mean", #16
    #"RH_min", #17
    #"RH_min_time" #18
]

wind_solar_columns=[
    "days", #0
    "mean_wind", #1
    "max_wind", #2
    "max_wind_direction",#3
    "max_wind_time", #4
    "maxspan_wind", #5
    "maxspan_wind_direction", #6
    "maxspan_wind_time", #7
    "mode_wind_direction", #8
    "sunlight_time", #9
    "solar_rad_flux", #10
    "snow_fall", #11
    "snow_cover", #12
    "cloud_cover", #13
    "wheather_am", #14
    "wheather_pm" #15
]

def get_daily_obs(prec_no, block_no, datetime):
    year=datetime.year
    month=datetime.month
    
    trans=str.maketrans({")":"", "]":"","×":"NaN"})
    url=f"https://www.data.jma.go.jp/obd/stats/etrn/view/daily_s1.php?prec_no={prec_no}&block_no={block_no}&year={year}&month={month:02d}&day=1&view=a1"
    html=urllib.request.urlopen(url).read()
    soup=BeautifulSoup(html)
    trs=soup.find("table",{"class":"data2_s","id":"tablefix1"})
    dfall=pd.DataFrame(columns=columns)
    for idx,tr in enumerate(trs.find_all("tr")[4:]):
        
        tds=tr.find_all("td")
        #デコード
        days=int(tds[0].string)
        Ps_mean=tds[1].string
        P0_mean=tds[2].string
        #P0_min=float(tds[3].string.translate(trans))
        #P0_min_time=tds[4].string
        precip_total=tds[5].string
        temp_mean=tds[10].string
        temp_max=tds[11].string
        temp_min=tds[13].string
        dfall.loc[idx,columns]=[days,Ps_mean, P0_mean, precip_total, temp_mean, temp_max, temp_min]
    dfall["year"]=year
    dfall["month"]=month
    return dfall

def get_daily_obs_wind_solar(prec_no, block_no, datetime, debug=False):
    year=datetime.year
    month=datetime.month
    
    trans=str.maketrans({")":"", "]":"","×":"NaN"})
    url=f"https://www.data.jma.go.jp/obd/stats/etrn/view/daily_s1.php?prec_no={prec_no}&block_no={block_no}&year={year}&month={month:02d}&day=1&view=a4"
    html=urllib.request.urlopen(url).read()
    soup=BeautifulSoup(html)
    trs=soup.find("table",{"class":"data2_s","id":"tablefix1"})
    dfall=pd.DataFrame(columns=wind_solar_columns)
    if debug:
        return trs
    for idx,tr in enumerate(trs.find_all("tr")[4:]):
        
        tds=tr.find_all("td")
        #デコード
        days=int(tds[0].string)
        param_list=[]
        for i in range(1,15+1):
            param_list.append(tds[i].string)
            
        dfall.loc[idx,wind_solar_columns]=[days] + param_list
    dfall["year"]=year
    dfall["month"]=month
    return dfall




#####################
#   monthly
#####################
def get_monthly_obs(prec_no, block_no, var="temp"):
    var_ids={"temp":"a1","precip":"p5"}
    url=f"https://www.data.jma.go.jp/obd/stats/etrn/view/monthly_s3.php?prec_no={prec_no}&block_no={block_no}&year=&month=&day=&view={var_ids[var]}"
    #データ取得
    html=urllib.request.urlopen(url).read()
    soup=BeautifulSoup(html)
    trs=soup.find("table",{"class":"data2_s","id":"tablefix1"})

    #データ格納
    dfall=pd.DataFrame(columns=np.arange(1,12+1,1))
    for idx,tr in enumerate(trs.find_all("tr")[1:]):
        tds=tr.find_all("td")
        year=tr.find_all("a")[0].string
        
        val=[t.string for t  in tds[1:-1]]
        val=[t.replace(")","").replace("]","").replace("\u3000","").replace('×',"").replace("--","") for t in val]
        dfall.loc[year,:]=val
        dfall.replace("",np.nan,inplace=True)
        dfall=dfall.astype("float")
    dfall=pd.DataFrame(dfall.stack()).reset_index().sort_values(["level_0","level_1"])
    dfall["yyyymm"]=pd.to_datetime(dfall["level_0"].astype(str)+"-"+dfall["level_1"].astype(str)
                             ,format="%Y-%m"     )
    dfall.rename(columns={0:block_no},inplace=True)
    dfall.set_index("yyyymm",inplace=True)
    
    return dfall[[block_no]]
