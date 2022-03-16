#! /home/oonishi/miniconda3/envs/ml/bin/python

import pygrib
import numpy as np
import pandas as pd
import xarray as xr
import argparse
import yaml
import glob
import os
import sqlite3
import sys
root_dir = [os.path.dirname(__file__), os.pardir]
sys.path.append(os.path.join(*root_dir))

from Common.database_upsert import upsert  # noqa: E402
from Common.logger import logger  # noqa: E402


LAT = np.arange(90, -90.1, -1.25)
LON = np.arange(0, 358.751, 1.25)

encode_fmt = {"dtype": "float32", "complevel": 5, "zlib": True}
encoding = {var: encode_fmt for var in ["rr", "T2m"]}

# girb2-> netcdf
MEMBERS = ["cntl", "pos_p1", "neg_p1", "pos_p2", "neg_p2"]
N_MEMBERS = 5
N_TIMESTEP = 240
N_LAT = 145
N_LON = 288
SHAPE = (N_MEMBERS, N_TIMESTEP, N_LAT, N_LON)
with open("./gpvpath.yaml") as f:
    CONFIG = yaml.safe_load(f)
DATADIR = CONFIG["DATADIR"]+"/CPS3"
OUTDIR = CONFIG["CPS3_NCDIR"]

# for netcdf -> csv
master = pd.read_excel("./master/JMA_situ_obs_master.xlsx")
master.set_index("IndexNbr", inplace=True)
targets = ["NAGOYA", "SAPPORO", "TOKYO", "HIROSHIMA",
           "MATSUYAMA", "SENDAI", "OSAKA", "NIIGATA", "FUKUOKA"]
master = master[master["StationName"].isin(targets)]


def cps3_grib2_to_nc(init_date: pd._libs.tslibs.timestamps.Timestamp, is_output: bool):
    """
    Args:
        initdate (pd.datetime) : cps3の初期時刻
        is_output(boolean) : netcdfに保存するか？
    Returns:
        xr.DataSet: cps3をgrib2からデコードしたもの
    """
    # ファイルopen
    yyyymmddhhmmss = init_date.strftime("%Y%m%d%H%M00")
    datetimes = pd.date_range(init_date, init_date +
                              pd.offsets.Day(N_TIMESTEP-1), freq="D")
    t_val = np.zeros(SHAPE, dtype=np.float32)
    rr_val = np.zeros(SHAPE, dtype=np.float32)

    logger.info(f"loading cps3 grib2 @{yyyymmddhhmmss}")
    buf_temp = pygrib.open(
        f"{DATADIR}/Z__C_RJTD_{yyyymmddhhmmss}_EPSC_MGPV_Rgl_Gll1p25deg_Lh2_Ptt_Emb_grib2.bin")
    buf_precip = pygrib.open(
        f"{DATADIR}/Z__C_RJTD_{yyyymmddhhmmss}_EPSC_MGPV_Rgl_Gll1p25deg_Lsurf_Prr_Emb_grib2.bin")
    try:
        # データ読み込み
        for timestep, dt in enumerate(datetimes):
            temp_list = buf_temp.select(validDate=dt)
            precip_list = buf_precip.select(validDate=dt)

            for i, (tt, rr) in enumerate(zip(temp_list, precip_list)):
                # for i,(tt, rr,rh, pp, uu,vv, sst) in enumerate(zip(temp_list, precip_list, rh_list,pp_list,uu_list, vv_list,sst_list)):
                t_val[i, timestep, :, :] = tt.values-273.15
                rr_val[i, timestep, :, :] = rr.values

        # netcdf化
        dims = ["member", "time", "lat", "lon"]
        values = {"rr": (dims,  rr_val, {"units": "mm/day", "long_name": "daily mean precipitation"}),
                  "T2m": (dims, t_val,  {"units": "degC", "long_name": "daily mean 2m temperature"}),
                  }
        coords = {
            "member": MEMBERS,
            "time": datetimes,
            "lat": ("lat", LAT, {"units": "degrees_north"}),
            "lon": ("lon", LON, {"units": "degrees_east"})
        }
        attrs = {"title": "CPS3 6month ensemble forecast daily",
                 "editor": "Tsunagu Community Analytics"}
        ds = xr.Dataset(values, coords, attrs)
        if is_output:
            filename=f"Glob_JMA_CPS3_tt_rr_{yyyymmddhhmmss}.nc"
            ds.to_netcdf(
                f"{OUTDIR}/{filename}", encoding=encoding)
            logger.info(f"save to {OUTDIR}/{filename}")
        # メモリクリア
        buffers = [buf_temp, buf_precip]
        for buf in buffers:
            buf.close()
        logger.info("decode end")
    except BaseException:
        logger.error("decode failed")
        raise
    return ds


def get_dataframe(ds: xr.Dataset, tlon: float, tlat: float, stname: str):
    """netcdfをpd.DataFrameにする

    Args:
        ds (xr.Dataset): netcdf化したcps
        tlon (float):切り出し地点の経度
        tlat (float): 切り出し地点の緯度
        stname (str): 切り出し地点名

    Returns:
        df (pd.DataFrame): 地点の予報値
        init_time (pd.Datatime64): 予報の初期時刻
    """

    # 日のデータだけ切り出す
    init_time = pd.to_datetime(ds["time"].values[0])
    df = ds[["rr", "T2m"]].interp(
        lat=tlat, lon=tlon).to_dataframe().reset_index()
    df["pred_month"] = df["time"].dt.month
    df["pred_month"] = df["pred_month"].apply(
        lambda x: x+12 if x < init_time.month else x)
    df["elapse_month"] = df["pred_month"]-init_time.month
    df["init_time"] = init_time
    df.drop(columns=["pred_month"], inplace=True)
    df.rename(columns={"time": "date_time"}, inplace=True)
    df["situ_name"] = stname
    df["init_date"] = pd.to_datetime(df["init_time"]).dt.strftime("%Y%m%d")
    df.drop(columns=["init_time"], inplace=True)
    return df, init_time


def netcdf_to_csv(ds: xr.Dataset, is_output: bool):
    """GPVから地点の気象データを切り出す

    Args:
        ds (xr.Dataset): netcdf化したcps3
        is_output (bool):csvに保存するかどうか

    Returns:
        dict: 地点名をキーとしたdfの辞書
    """
    logger.info("Extract station point data")
    try:
        dfdict = {}
        for tlon, tlat, stname, idx in zip(master["lon"], master["lat"],
                                           master["StationName"], master.index):
            dfout, init_time = get_dataframe(ds, tlon, tlat, stname)
            dfdict[stname] = dfout
            init_date = init_time.strftime("%Y%m%d%H%M00")
            if is_output:
                dirname = CONFIG["CPS3_CSVDIR"] + \
                    f"/{stname}/{init_time.year:04d}/{init_time.month:02d}"
                filename=f"cps3_{stname}_{idx}_{init_date}.csv"
                os.makedirs(dirname, exist_ok=True)
                
                dfout.to_csv(
                    f"{dirname}/{filename}", index=None)
                logger.info(f"save to {dirname}/{filename}")
    except BaseException as e:
        logger.error("Extract failed")
        raise
    return dfdict


def upsert_cps(df, CONNECTION_CONFIG):
    logger.info("upserting...")
    upsert(df, "cps_seasonal_forecast", [
        "date_time", "situ_name", "init_date"], CONNECTION_CONFIG)
    return 0


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("init_date", help="季節予報GPVの初期値, YYYYMMDDhhmm")

    # ファイル出力系の引数
    parser.add_argument("--output_nc", action="store_true")
    parser.add_argument("--output_csv", action="store_true")
    parser.add_argument("--output_db", action="store_true")

    # 処理の途中から始めるフラグ
    parser.add_argument("--from_nc", action="store_true",
                        help="netcdf-> csvの変換から処理を開始")
    parser.add_argument("--from_csv", action="store_true",
                        help="csv -> dbへの登録のみ実行")

    args = parser.parse_args()

    # grib2 -> nc
    init_date = pd.to_datetime(args.init_date, format="%Y%m%d%H%M")
    if args.from_nc:
        #すでにncファイルはある
        yyyymmddhhmmss = init_date.strftime("%Y%m%d%H%M00")
        ds = xr.open_dataset(CONFIG["CPS3_NCDIR"] +
                             f"/Glob_JMA_CPS3_tt_rr_{yyyymmddhhmmss}.nc")
    elif args.from_csv:
        #
        logger.info("Skip extracting part ")
    else:
        ds = cps3_grib2_to_nc(init_date, args.output_nc)

    # nc -> csv
    if args.from_csv:
        yyyymmddhhmmss = init_date.strftime("%Y%m%d%H%M00")
        files = glob.glob(
            CONFIG["CPS3_CSVDIR"]+f"/*/{init_date.year:04d}/{init_date.month:02d}/cps3_*_*_{yyyymmddhhmmss}.csv")
        dflist = []
        for f in files:
            dflist.append(pd.read_csv(f))
        dfall = pd.concat(dflist)
    else:
        dfdict = netcdf_to_csv(ds, args.output_csv)
        dflist = list(dfdict.values())
        dfall = pd.concat(dflist)

    # csv -> db
    if args.output_db:
        logger.info(f"Connecting to {CONFIG['DB_DIR']}")
        CONNECTION_CONFIG = sqlite3.connect(CONFIG["DB_DIR"])
        upsert_cps(dfall,CONNECTION_CONFIG)
        CONNECTION_CONFIG.close()
