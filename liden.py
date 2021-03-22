#!/usr/bin/env python
# coding: utf-8
import numpy as np
import struct
import json
from datetime import datetime,timedelta
from io import BufferedReader,BytesIO
class Liden():
    """
    load LIDEN binary data, which is provided by JMSBC

    Attributes
    -----------
    dt datetime: datetime of LIDEN data(UTC)
    timezone string: timezone of LIDEN data(UTC)
    pitch int:  
    num int: the number of lightning in the data
    dtype numpy.dtype: structure of the LIDEN data without header info
    elapsetime numpy.ndarray: elapsetime[ms] after the LIDEN data start recording
    lat numpy.ndarray:the point of lightning
    lon numpy.ndarray:the point of lightning
    multiplicity numpy.ndarray: lightning multiplicity of each lightning
    lightningtype numpy.ndarray:
    
    """
    def __init__(self,f):
        """
        f: _io.BufferedReader
        """
        self.timezone="UTC"
        f.read(18) #skip system header
        year =struct.unpack(">H",f.read(2))[0]
        mmdd=struct.unpack(">H",f.read(2))[0]
        month=int(mmdd/100)
        day=int(mmdd-month*100)

        hhmm=struct.unpack(">H",f.read(2))[0]
        hour=int(hhmm/100)
        minute=int(hhmm-hour*100)
        sec=struct.unpack(">H",f.read(2))[0]
        self.dt = datetime(year,month,day,hour,minute)
        self.pitch=struct.unpack(">H",f.read(2))[0]
        self.num =struct.unpack(">H",f.read(2))[0]
        yobi=struct.unpack(">cccc",f.read(4))[0]
        self.dtype=np.dtype([
            ("elapsetime",">u2"),
            ("lat",">u2"),
            ("lon",">u2"),
            ("multiplicity",">u2"),
            ("space",">u2")
        ])
        
        if isinstance(f,BufferedReader):
            chunk=np.fromfile(f,dtype=self.dtype,count=self.num)
        elif isinstance(f,BytesIO):
            chunk=np.frombuffer(f.read(),dtype=self.dtype,count=self.num)
        self.elapsetime=chunk[:]["elapsetime"]*10 # x 1e-2 x 1e3 [ms]
        self.lat=chunk[:]["lat"]*1e-3
        self.lon=chunk[:]["lon"]*1e-3 +100
        self.multiplicity=(chunk[:]["multiplicity"]/100).astype(np.int64)
        self.lightningtype=chunk[:]["multiplicity"]-self.multiplicity*100
        datetimes=[np.datetime64(self.dt ,"ms") + np.timedelta64(milisec,"ms" ) \
                   for milisec in self.elapsetime
        ] 
        self.datetimeindex=np.array(datetimes)
        f.close()
    def utc2jst(self):
        """
        convert timezone from UTC to JST when timezone is UTC 
        """
        if self.timezone=="UTC":
            self.dt +=timedelta(hours=9)
            self.timezone="JST"
            if self.num !=0:
                self.datetimeindex+=np.timedelta64(32400000)
        else:
            print("timezone must be UTC but now {}".format(self.timezone))        
    def jst2utc(self):
        """
        convert timezone from JST to UTC when timezone is JST
        """
        if self.timezone=="JST":
            self.dt += timedelta(hours=-9)
            self.timezone="UTC"
            if self.num !=0: 
                self.datetimeindex-=np.timedelta64(32400000)
        else:
            print("timezone must be JST but now {}".format(self.timezone))
    def to_lls(self,outputfile):
        """
        covert liden format to ls format
        Parameter
        ------------
        outputfile string:
        
        """
        pass
    def to_json(self,filepath):
        """
        convert liden data to json

        """
        out=self.to_dict()
        self.printInfo()
        with open(filepath,"w") as f:
            json.dump(out,f,indent=4)
        pass
        
    def to_dict(self):
        """
        convert liden data to_dict

        """
        if self.num==0:
            strdatetimeindex=[]
        else:
            strdatetimeindex=[dt.isoformat() for dt in self.datetimeindex.tolist()]

        out={
            #body
            "datetime":strdatetimeindex,
            "lon":self.lon.tolist(),
            "lat":self.lat.tolist(),
            "multiplicity":self.multiplicity.tolist(),
            "lightningtype":self.lightningtype.tolist()
        }
        return out
       
    def to_list(self,timestep=0):
        """
        return liden data as list 
        (datetime,lat,lon,multiplicity,lightningtype)
        parameter
        ---------------
        timestep int:
        """
        if self.num==0:
            print("This data is empty")
            return 0
        out = [self.datetimeindex[timestep],self.lat[timestep],
               self.lon[timestep],self.multiplicity[timestep],
               self.lightningtype[timestep]]
        return out
    def printInfo(self):
        print("[header info]")
        print("timezone:{}".format(self.timezone))
        print("datetime:{}".format(self.dt.strftime("%Y-%m-%d %H:%M:%S")))
        print("numbers:{}".format(self.num))
        print("pitch:{}".format(self.pitch))       
    
def read_liden(file):
    if isinstance(file,str):
        f=open(file,"br")
        return Liden(f)
    elif isinstance(file,BytesIO):
        return Liden(file)
    else:
        raise ValueError
