#! /usr/bin/env python
from petalib import ticommon
import numpy as np
import datetime
import pandas as pd
import sys,os,inspect
currentdir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
parentdir = os.path.dirname(currentdir)
sys.path.insert(0,parentdir) 
from common import utilities as du
from petalib import custom as cti
import math
#period, inputtype, inputperiod,symbol

class BBH(ticommon.TI):
    NAME = "BBH"

    def __init__(self, period=20,slopeperiod=14, inputtype="ohlc",inputperiod=3,symbol="NIFTY",suffix ='1',middleband="middle_band" ,stddev="std_dev",upper_band="upper_band",
                 lower_band="lower_band", bb_column_name='bandwidth',perc_bb_column_name='b_perc',bol_bandwith='bollinger_bandwith',high_source_column='high',
                 low_source_column='low', close_source_column='close',bb_slope_col='bb_slope',normalised_column='normalized_bollinger_bandwith',normalised_slope= 'normalized_bb_slope'):
				 
        super(BBH, self).__init__( True)
        self._period = period
        self._suffix = suffix
        self._stateful = True
        self._name = self.NAME + "_" + str(period) + "_src_" + inputtype + "_" + str(inputperiod)
        self._logger = du.getlogger(self._name) #??
        self.high_source_column = high_source_column
        self.low_source_column = low_source_column
        self.close_source_column = close_source_column
		
        self.bol_bandwith = bol_bandwith
        self.perc_bb_column_name = perc_bb_column_name
        self.middle_band = str(middleband)+"_"+str(period)
        self.stddev=str(stddev)+"_"+str(period)
        self.stddev=du.get_column_name(self.stddev,inputtype,inputperiod,symbol,suffix)
        #self.middle_band = str(middle_band)+"_"+str(period)
        self.upper_band=str(upper_band)+"_"+str(period)
        self.lower_band=str(lower_band)+"_"+str(period)
        
        self.bb_column_name = bb_column_name +"_"+str(period)
        self.bb_slope_col= bb_slope_col
        self.normalised_column = str(normalised_column)
        self.normalised_slope = normalised_slope
        self.slopeperiod = slopeperiod
        
		#-->DT_Multiple setting support
        self.adjustcolumnnames(period,inputtype,inputperiod,symbol,suffix)
		#<---
	   
        customindargsdict=self.getcustomindicatorargsdict()
        self.customtilist= cti.getcustom_new(customindargsdict)
        self.fieldnames.extend(
            [self.middle_band, self.stddev, self.upper_band, self.lower_band, self.bb_column_name, self.perc_bb_column_name,
             self.bol_bandwith,self.normalised_column])
        cti.extendfieldnames(self.customtilist, self.fieldnames)
		
		
    #-->DT_Multiple setting support 
	#adjust column names as per settings
    def adjustcolumnnames(self,period,inputtype,inputperiod,symbol,suffix):
        self.bol_bandwith = du.get_column_name(self.bol_bandwith,inputtype,inputperiod,symbol,suffix)
        self.perc_bb_column_name = du.get_column_name(self.perc_bb_column_name,inputtype,inputperiod,symbol,suffix)
        self.middle_band = du.get_column_name(self.middle_band,inputtype,inputperiod,symbol,suffix)
        self.upper_band=du.get_column_name(self.upper_band,inputtype,inputperiod,symbol,suffix)
        self.lower_band=du.get_column_name(self.lower_band,inputtype,inputperiod,symbol,suffix)
        self.bb_column_name = du.get_column_name(self.bb_column_name,inputtype,inputperiod,symbol,suffix)
        self.bb_slope_col =du.get_column_name(self.bb_slope_col,inputtype,inputperiod,symbol,suffix)
        self.normalised_column =du.get_column_name(self.normalised_column,inputtype,inputperiod,symbol,suffix)
        self.normalised_slope =du.get_column_name(self.normalised_slope,inputtype,inputperiod,symbol,suffix)
		
    def getcustomindicatorargsdict(self):
        dict={}
        dict["slope"]=[(self.bol_bandwith,self.bb_slope_col,self.slopeperiod),(self.normalised_column,self.normalised_slope,self.slopeperiod)]
        return dict

    def adjustdatatypes(self):
        self._state.index = self._state['date_time']
        self._state[self.middle_band] = self._state[self.middle_band].astype(float)
        self._state[self.low_source_column] = self._state[self.low_source_column].astype(float)
        self._state[self.high_source_column] = self._state[self.high_source_column].astype(float)
        self._state[self.close_source_column] = self._state[self.close_source_column].astype(float)
        self._state[self.stddev] = self._state[self.stddev].astype(float)
        self._state[self.lower_band] = self._state[self.lower_band].astype(float)
        self._state[self.bb_column_name] = self._state[self.bb_column_name].astype(float)
        self._state[self.perc_bb_column_name] = self._state[self.perc_bb_column_name].astype(float)
        self._state[self.bol_bandwith] = self._state[self.bol_bandwith].astype(float)
        self._state[self.normalised_column] = self._state[self.normalised_column].astype(float)
        if "bb_slope" in self._state: self._state["bb_slope"] = self._state["bb_slope"].astype(float)
        self._saveintermediatestate(self._state)

    def setvaluesforrowbyindex(self, df, tdf, datetime):
        smavalue = self._smavalue(tdf, datetime)
        stdvalue = self._stdvalue(tdf, datetime, smavalue)
        df[self.middle_band][datetime] = smavalue
        df[self.stddev][datetime] = stdvalue
        df[self.upper_band][datetime] = smavalue + 2 * stdvalue
        df[self.lower_band][datetime] = smavalue - 2 * stdvalue
        df[self.bb_column_name][datetime] = 4 * stdvalue
        b_perc = (df[self.high_source_column][datetime] - df[self.lower_band][
            datetime]) / (df[self.upper_band][datetime] - df[self.lower_band][datetime])
        b_perc=b_perc*100
        df[self.perc_bb_column_name][datetime] =b_perc
        tdf[self.perc_bb_column_name][datetime] =b_perc
        df[self.bol_bandwith][datetime] = ((df[self.upper_band][datetime] - df[self.lower_band][datetime]) /
                                           df[self.middle_band][datetime]) * 100
        tdf[self.bol_bandwith][datetime]=df[self.bol_bandwith][datetime]
        df[self.normalised_column][datetime] = (df[self.bol_bandwith][datetime] *10000) /df[self.high_source_column][datetime]
        tdf[self.normalised_column][datetime]=df[self.normalised_column][datetime]
        
        cti.setvaluesforrowbyindex(self.customtilist, df, tdf, datetime)

    def _smavalue(self, tdf, datetime):
        rowno = tdf.index.get_loc(datetime)
        if rowno ==0: return np.NaN
        sma=tdf[self.middle_band].iloc[rowno]
        if np.isfinite(float(sma)): return sma
        subset = tdf[rowno - (self._period - 1):rowno + 1]
        currsmavalue = (subset[self.high_source_column].mean())
        tdf[self.middle_band].iloc[rowno] = currsmavalue
        return currsmavalue

    def _stdvalue(self, tdf, datetime, smavalue):
        rowno = tdf.index.get_loc(datetime)
        if rowno == 0: return np.NaN
        std = tdf[self.stddev].iloc[rowno]
        if np.isfinite(float(std)): return std
        subset = tdf[rowno - (self._period - 1):rowno + 1]
        currstdvalue = (subset[self.high_source_column].std())
        tdf[self.stddev].iloc[rowno] = currstdvalue
        return currstdvalue

    def _bbvalue(self, tdf, datetime, smavalue, stdvalue):
        bbvalue = tdf[self.stdev][datetime]
        if np.isfinite(float(bbvalue)): return bbvalue
        tdf[self.bb_column_name][datetime] = (tdf[self.upper_band][datetime] -tdf[self.lower_band][datetime])
        return tdf[self.bb_column_name][datetime]
	
	

    def _addtargetcolumns(self, df):
        if self.middle_band not in df: df[self.middle_band] = np.NaN
        if self.upper_band not in df: df[self.upper_band] = np.NaN
        if self.bb_column_name not in df: df[self.bb_column_name] = np.NaN
        if self.lower_band not in df: df[self.lower_band] = np.NaN
        if self.perc_bb_column_name not in df: df[self.perc_bb_column_name] = np.NaN
        if self.bol_bandwith not in df: df[self.bol_bandwith] = np.NaN
        if self.normalised_column not in df: df[self.normalised_column]=np.NaN
        if self.stddev not in df: df[self.stddev] = np.NaN
        df = cti._addtargetcolumns(self.customtilist, df)
        return df

    def _statelessindicator(self, df):
        subset = df[df[self.high_source_column].notnull()].head(self._period)
        datetime = subset['date_time'].iloc[self._period - 1]
        rowno = df.index.get_loc(datetime)
        sma = subset[self.high_source_column].mean()
        std = subset[self.high_source_column].std()
        df[self.middle_band].iloc[rowno] = sma
        df[self.stddev].iloc[rowno] = std
        df[self.upper_band] = sma + 2 *std
        df[self.lower_band] = sma - 2*std
        df[self.bb_column_name].iloc[rowno] = 4 * std
        df = cti._statelessindicator(self.customtilist, df)
        return self._statefulindicator(df)


    def _saveintermediatestate(self, df):
        if self._period >= int(self.slopeperiod) :
            self._state=self._state.append(df.tail(self._period))
            self._state = self._state.tail(self._period)
            #print('period is greater than slopeperiod')
        else:
            self._state=self._state.append(df.tail(int(self.slopeperiod)))
            self._state = self._state.tail(int(self.slopeperiod)) 
            #print('slopeperiod is greater than period')
        cti._saveintermediatestate(self.customtilist,df)
    def parameters(self):
        return str(self._period)+"_"+self.slopeperiod
	
	
    
    
class BBC(ticommon.TI):
    NAME = "BBC"

    def __init__(self, period=20,slopeperiod=14, inputtype="ohlc",inputperiod=3,symbol="NIFTY",suffix ='1',middleband="middle_band" ,stddev="std_dev",upper_band="upper_band",
                 lower_band="lower_band", bb_column_name='bandwidth',perc_bb_column_name='b_perc',bol_bandwith='bollinger_bandwith',high_source_column='high',
                 low_source_column='low', close_source_column='close',bb_slope_col='bb_slope',normalised_column='normalized_bollinger_bandwith',normalised_slope= 'normalized_bb_slope'):
				 
        super(BBC, self).__init__( True)
        self._period = period
        self._suffix = suffix
        self._stateful = True
        self._name = self.NAME + "_" + str(period) + "_src_" + inputtype + "_" + str(inputperiod)
        self._logger = du.getlogger(self._name) #??
        self.high_source_column = high_source_column
        self.low_source_column = low_source_column
        self.close_source_column = close_source_column
    
		
        self.bol_bandwith = bol_bandwith
        self.perc_bb_column_name = perc_bb_column_name
        self.middle_band = str(middleband)+"_"+str(period)
        self.stddev=str(stddev)+"_"+str(period)
        self.stddev=du.get_column_name(self.stddev,inputtype,inputperiod,symbol,suffix)
        #self.middle_band = str(middle_band)+"_"+str(period)
        self.upper_band=str(upper_band)+"_"+str(period)
        self.lower_band=str(lower_band)+"_"+str(period)
        
        self.bb_column_name = bb_column_name +"_"+str(period)
        self.bb_slope_col= bb_slope_col
        self.normalised_column = str(normalised_column)
        self.normalised_slope = normalised_slope
        self.slopeperiod = slopeperiod
        
		#-->DT_Multiple setting support
        self.adjustcolumnnames(period,inputtype,inputperiod,symbol,suffix)
		#<---
	   
        customindargsdict=self.getcustomindicatorargsdict()
        self.customtilist= cti.getcustom_new(customindargsdict)
        self.fieldnames.extend(
            [self.middle_band, self.stddev, self.upper_band, self.lower_band, self.bb_column_name, self.perc_bb_column_name,
             self.bol_bandwith,self.normalised_column])
        cti.extendfieldnames(self.customtilist, self.fieldnames)
		
		
    #-->DT_Multiple setting support 
	#adjust column names as per settings
    def adjustcolumnnames(self,period,inputtype,inputperiod,symbol,suffix):
        self.bol_bandwith = du.get_column_name(self.bol_bandwith,inputtype,inputperiod,symbol,suffix)
        self.perc_bb_column_name = du.get_column_name(self.perc_bb_column_name,inputtype,inputperiod,symbol,suffix)
        self.middle_band = du.get_column_name(self.middle_band,inputtype,inputperiod,symbol,suffix)
        self.upper_band=du.get_column_name(self.upper_band,inputtype,inputperiod,symbol,suffix)
        self.lower_band=du.get_column_name(self.lower_band,inputtype,inputperiod,symbol,suffix)
        self.bb_column_name = du.get_column_name(self.bb_column_name,inputtype,inputperiod,symbol,suffix)
        self.bb_slope_col =du.get_column_name(self.bb_slope_col,inputtype,inputperiod,symbol,suffix)
        self.normalised_column =du.get_column_name(self.normalised_column,inputtype,inputperiod,symbol,suffix)
        self.normalised_slope =du.get_column_name(self.normalised_slope,inputtype,inputperiod,symbol,suffix)
		
    def getcustomindicatorargsdict(self):
        dict={}
        dict["slope"]=[(self.bol_bandwith,self.bb_slope_col,self.slopeperiod),(self.normalised_column,self.normalised_slope,self.slopeperiod)]
        return dict

    def adjustdatatypes(self):
        self._state.index = self._state['date_time']
        self._state[self.middle_band] = self._state[self.middle_band].astype(float)
        self._state[self.low_source_column] = self._state[self.low_source_column].astype(float)
        self._state[self.high_source_column] = self._state[self.high_source_column].astype(float)
        self._state[self.close_source_column] = self._state[self.close_source_column].astype(float)
        self._state[self.stddev] = self._state[self.stddev].astype(float)
        self._state[self.lower_band] = self._state[self.lower_band].astype(float)
        self._state[self.bb_column_name] = self._state[self.bb_column_name].astype(float)
        self._state[self.perc_bb_column_name] = self._state[self.perc_bb_column_name].astype(float)
        self._state[self.bol_bandwith] = self._state[self.bol_bandwith].astype(float)
        self._state[self.normalised_column] = self._state[self.normalised_column].astype(float)
        if "bb_slope" in self._state: self._state["bb_slope"] = self._state["bb_slope"].astype(float)
        self._saveintermediatestate(self._state)

    def setvaluesforrowbyindex(self, df, tdf, datetime):
        smavalue = self._smavalue(tdf, datetime)
        stdvalue = self._stdvalue(tdf, datetime, smavalue)
        df[self.middle_band][datetime] = smavalue
        df[self.stddev][datetime] = stdvalue
        df[self.upper_band][datetime] = smavalue + 2 * stdvalue
        df[self.lower_band][datetime] = smavalue - 2 * stdvalue
        df[self.bb_column_name][datetime] = 4 * stdvalue
        b_perc = (df[self.close_source_column][datetime] - df[self.lower_band][
            datetime]) / (df[self.upper_band][datetime] - df[self.lower_band][datetime])
        b_perc=b_perc*100
        df[self.perc_bb_column_name][datetime] =b_perc
        tdf[self.perc_bb_column_name][datetime] =b_perc
        df[self.bol_bandwith][datetime] = ((df[self.upper_band][datetime] - df[self.lower_band][datetime]) /
                                           df[self.middle_band][datetime]) * 100
        tdf[self.bol_bandwith][datetime]=df[self.bol_bandwith][datetime]
        df[self.normalised_column][datetime] = (df[self.bol_bandwith][datetime] *10000) /df[self.close_source_column][datetime]
        tdf[self.normalised_column][datetime]=df[self.normalised_column][datetime]
        
        cti.setvaluesforrowbyindex(self.customtilist, df, tdf, datetime)

    def _smavalue(self, tdf, datetime):
        rowno = tdf.index.get_loc(datetime)
        if rowno ==0: return np.NaN
        sma=tdf[self.middle_band].iloc[rowno]
        if np.isfinite(float(sma)): return sma
        subset = tdf[rowno - (self._period - 1):rowno + 1]
        currsmavalue = (subset[self.close_source_column].mean())
        tdf[self.middle_band].iloc[rowno] = currsmavalue
        return currsmavalue

    def _stdvalue(self, tdf, datetime, smavalue):
        rowno = tdf.index.get_loc(datetime)
        if rowno == 0: return np.NaN
        std = tdf[self.stddev].iloc[rowno]
        if np.isfinite(float(std)): return std
        subset = tdf[rowno - (self._period - 1):rowno + 1]
        currstdvalue = (subset[self.close_source_column].std())
        tdf[self.stddev].iloc[rowno] = currstdvalue
        return currstdvalue

    def _bbvalue(self, tdf, datetime, smavalue, stdvalue):
        bbvalue = tdf[self.stdev][datetime]
        if np.isfinite(float(bbvalue)): return bbvalue
        tdf[self.bb_column_name][datetime] = (tdf[self.upper_band][datetime] -tdf[self.lower_band][datetime])
        return tdf[self.bb_column_name][datetime]
	
	

    def _addtargetcolumns(self, df):
        if self.middle_band not in df: df[self.middle_band] = np.NaN
        if self.upper_band not in df: df[self.upper_band] = np.NaN
        if self.bb_column_name not in df: df[self.bb_column_name] = np.NaN
        if self.lower_band not in df: df[self.lower_band] = np.NaN
        if self.perc_bb_column_name not in df: df[self.perc_bb_column_name] = np.NaN
        if self.bol_bandwith not in df: df[self.bol_bandwith] = np.NaN
        if self.normalised_column not in df: df[self.normalised_column]=np.NaN
        if self.stddev not in df: df[self.stddev] = np.NaN
        df = cti._addtargetcolumns(self.customtilist, df)
        return df

    def _statelessindicator(self, df):
        subset = df[df[self.close_source_column].notnull()].head(self._period)
        datetime = subset['date_time'].iloc[self._period - 1]
        rowno = df.index.get_loc(datetime)
        sma = subset[self.close_source_column].mean()
        std = subset[self.close_source_column].std()
        df[self.middle_band].iloc[rowno] = sma
        df[self.stddev].iloc[rowno] = std
        df[self.upper_band] = sma + 2 *std
        df[self.lower_band] = sma - 2*std
        df[self.bb_column_name].iloc[rowno] = 4 * std
        df = cti._statelessindicator(self.customtilist, df)
        return self._statefulindicator(df)


    def _saveintermediatestate(self, df):
        if self._period >= int(self.slopeperiod) :
            self._state=self._state.append(df.tail(self._period))
            self._state = self._state.tail(self._period)
            #print('period is greater than slopeperiod')
        else:
            self._state=self._state.append(df.tail(int(self.slopeperiod)))
            self._state = self._state.tail(int(self.slopeperiod)) 
            #print('slopeperiod is greater than period')
        cti._saveintermediatestate(self.customtilist,df)
    def parameters(self):
        return str(self._period)+"_"+self.slopeperiod
	
	
    
        
    
class BBL(ticommon.TI):
    NAME = "BBL"

    def __init__(self, period=20,slopeperiod=14, inputtype="ohlc",inputperiod=3,symbol="NIFTY",suffix ='1',middleband="middle_band" ,stddev="std_dev",upper_band="upper_band",
                 lower_band="lower_band", bb_column_name='bandwidth',perc_bb_column_name='b_perc',bol_bandwith='bollinger_bandwith',high_source_column='high',
                 low_source_column='low', close_source_column='close',bb_slope_col='bb_slope',normalised_column='normalized_bollinger_bandwith',normalised_slope= 'normalized_bb_slope'):
				 
        super(BBL, self).__init__( True)
        self._period = period
        self._suffix = suffix
        self._stateful = True
        self._name = self.NAME + "_" + str(period) + "_src_" + inputtype + "_" + str(inputperiod)
        self._logger = du.getlogger(self._name) #??
        self.high_source_column = high_source_column
        self.low_source_column = low_source_column
        self.close_source_column = close_source_column
		
        self.bol_bandwith = bol_bandwith
        self.perc_bb_column_name = perc_bb_column_name
        self.middle_band = str(middleband)+"_"+str(period)
        self.stddev=str(stddev)+"_"+str(period)
        self.stddev=du.get_column_name(self.stddev,inputtype,inputperiod,symbol,suffix)
        #self.middle_band = str(middle_band)+"_"+str(period)
        self.upper_band=str(upper_band)+"_"+str(period)
        self.lower_band=str(lower_band)+"_"+str(period)
        
        self.bb_column_name = bb_column_name +"_"+str(period)
        self.bb_slope_col= bb_slope_col
        self.normalised_column = str(normalised_column)
        self.normalised_slope = normalised_slope
        self.slopeperiod = slopeperiod
        
		#-->DT_Multiple setting support
        self.adjustcolumnnames(period,inputtype,inputperiod,symbol,suffix)
		#<---
	   
        customindargsdict=self.getcustomindicatorargsdict()
        self.customtilist= cti.getcustom_new(customindargsdict)
        self.fieldnames.extend(
            [self.middle_band, self.stddev, self.upper_band, self.lower_band, self.bb_column_name, self.perc_bb_column_name,
             self.bol_bandwith,self.normalised_column])
        cti.extendfieldnames(self.customtilist, self.fieldnames)
		
		
    #-->DT_Multiple setting support 
	#adjust column names as per settings
    def adjustcolumnnames(self,period,inputtype,inputperiod,symbol,suffix):
        self.bol_bandwith = du.get_column_name(self.bol_bandwith,inputtype,inputperiod,symbol,suffix)
        self.perc_bb_column_name = du.get_column_name(self.perc_bb_column_name,inputtype,inputperiod,symbol,suffix)
        self.middle_band = du.get_column_name(self.middle_band,inputtype,inputperiod,symbol,suffix)
        self.upper_band=du.get_column_name(self.upper_band,inputtype,inputperiod,symbol,suffix)
        self.lower_band=du.get_column_name(self.lower_band,inputtype,inputperiod,symbol,suffix)
        self.bb_column_name = du.get_column_name(self.bb_column_name,inputtype,inputperiod,symbol,suffix)
        self.bb_slope_col =du.get_column_name(self.bb_slope_col,inputtype,inputperiod,symbol,suffix)
        self.normalised_column =du.get_column_name(self.normalised_column,inputtype,inputperiod,symbol,suffix)
        self.normalised_slope =du.get_column_name(self.normalised_slope,inputtype,inputperiod,symbol,suffix)
		
    def getcustomindicatorargsdict(self):
        dict={}
        dict["slope"]=[(self.bol_bandwith,self.bb_slope_col,self.slopeperiod),(self.normalised_column,self.normalised_slope,self.slopeperiod)]
        return dict

    def adjustdatatypes(self):
        self._state.index = self._state['date_time']
        self._state[self.middle_band] = self._state[self.middle_band].astype(float)
        self._state[self.low_source_column] = self._state[self.low_source_column].astype(float)
        self._state[self.high_source_column] = self._state[self.high_source_column].astype(float)
        self._state[self.close_source_column] = self._state[self.close_source_column].astype(float)
        self._state[self.stddev] = self._state[self.stddev].astype(float)
        self._state[self.lower_band] = self._state[self.lower_band].astype(float)
        self._state[self.bb_column_name] = self._state[self.bb_column_name].astype(float)
        self._state[self.perc_bb_column_name] = self._state[self.perc_bb_column_name].astype(float)
        self._state[self.bol_bandwith] = self._state[self.bol_bandwith].astype(float)
        self._state[self.normalised_column] = self._state[self.normalised_column].astype(float)
        if "bb_slope" in self._state: self._state["bb_slope"] = self._state["bb_slope"].astype(float)
        self._saveintermediatestate(self._state)

    def setvaluesforrowbyindex(self, df, tdf, datetime):
        smavalue = self._smavalue(tdf, datetime)
        stdvalue = self._stdvalue(tdf, datetime, smavalue)
        df[self.middle_band][datetime] = smavalue
        df[self.stddev][datetime] = stdvalue
        df[self.upper_band][datetime] = smavalue + 2 * stdvalue
        df[self.lower_band][datetime] = smavalue - 2 * stdvalue
        df[self.bb_column_name][datetime] = 4 * stdvalue
        b_perc = (df[self.low_source_column][datetime] - df[self.lower_band][
            datetime]) / (df[self.upper_band][datetime] - df[self.lower_band][datetime])
        b_perc=b_perc*100
        df[self.perc_bb_column_name][datetime] =b_perc
        tdf[self.perc_bb_column_name][datetime] =b_perc
        df[self.bol_bandwith][datetime] = ((df[self.upper_band][datetime] - df[self.lower_band][datetime]) /
                                           df[self.middle_band][datetime]) * 100
        tdf[self.bol_bandwith][datetime]=df[self.bol_bandwith][datetime]
        df[self.normalised_column][datetime] = (df[self.bol_bandwith][datetime] *10000) /df[self.low_source_column][datetime]
        tdf[self.normalised_column][datetime]=df[self.normalised_column][datetime]
        
        cti.setvaluesforrowbyindex(self.customtilist, df, tdf, datetime)

    def _smavalue(self, tdf, datetime):
        rowno = tdf.index.get_loc(datetime)
        if rowno ==0: return np.NaN
        sma=tdf[self.middle_band].iloc[rowno]
        if np.isfinite(float(sma)): return sma
        subset = tdf[rowno - (self._period - 1):rowno + 1]
        currsmavalue = (subset[self.low_source_column].mean())
        tdf[self.middle_band].iloc[rowno] = currsmavalue
        return currsmavalue

    def _stdvalue(self, tdf, datetime, smavalue):
        rowno = tdf.index.get_loc(datetime)
        if rowno == 0: return np.NaN
        std = tdf[self.stddev].iloc[rowno]
        if np.isfinite(float(std)): return std
        subset = tdf[rowno - (self._period - 1):rowno + 1]
        currstdvalue = (subset[self.low_source_column].std())
        tdf[self.stddev].iloc[rowno] = currstdvalue
        return currstdvalue

    def _bbvalue(self, tdf, datetime, smavalue, stdvalue):
        bbvalue = tdf[self.stdev][datetime]
        if np.isfinite(float(bbvalue)): return bbvalue
        tdf[self.bb_column_name][datetime] = (tdf[self.upper_band][datetime] -tdf[self.lower_band][datetime])
        return tdf[self.bb_column_name][datetime]
	
	

    def _addtargetcolumns(self, df):
        if self.middle_band not in df: df[self.middle_band] = np.NaN
        if self.upper_band not in df: df[self.upper_band] = np.NaN
        if self.bb_column_name not in df: df[self.bb_column_name] = np.NaN
        if self.lower_band not in df: df[self.lower_band] = np.NaN
        if self.perc_bb_column_name not in df: df[self.perc_bb_column_name] = np.NaN
        if self.bol_bandwith not in df: df[self.bol_bandwith] = np.NaN
        if self.normalised_column not in df: df[self.normalised_column]=np.NaN
        if self.stddev not in df: df[self.stddev] = np.NaN
        df = cti._addtargetcolumns(self.customtilist, df)
        return df

    def _statelessindicator(self, df):
        subset = df[df[self.low_source_column].notnull()].head(self._period)
        datetime = subset['date_time'].iloc[self._period - 1]
        rowno = df.index.get_loc(datetime)
        sma = subset[self.low_source_column].mean()
        std = subset[self.low_source_column].std()
        df[self.middle_band].iloc[rowno] = sma
        df[self.stddev].iloc[rowno] = std
        df[self.upper_band] = sma + 2 *std
        df[self.lower_band] = sma - 2*std
        df[self.bb_column_name].iloc[rowno] = 4 * std
        df = cti._statelessindicator(self.customtilist, df)
        return self._statefulindicator(df)


    def _saveintermediatestate(self, df):
        if self._period >= int(self.slopeperiod) :
            self._state=self._state.append(df.tail(self._period))
            self._state = self._state.tail(self._period)
            #print('period is greater than slopeperiod')
        else:
            self._state=self._state.append(df.tail(int(self.slopeperiod)))
            self._state = self._state.tail(int(self.slopeperiod)) 
            #print('slopeperiod is greater than period')
        cti._saveintermediatestate(self.customtilist,df)
    def parameters(self):
        return str(self._period)+"_"+self.slopeperiod
	
	
    
       
class BBO(ticommon.TI):
    NAME = "BBO"

    def __init__(self, period=20,slopeperiod=14, inputtype="ohlc",inputperiod=3,symbol="NIFTY",suffix ='1',middleband="middle_band" ,stddev="std_dev",upper_band="upper_band",
                 lower_band="lower_band", bb_column_name='bandwidth',perc_bb_column_name='b_perc',bol_bandwith='bollinger_bandwith',high_source_column='high',
                 low_source_column='low', close_source_column='close',open_source_column='open',bb_slope_col='bb_slope',normalised_column='normalized_bollinger_bandwith',normalised_slope= 'normalized_bb_slope'):
				 
        super(BBO, self).__init__( True)
        self._period = period
        self._suffix = suffix
        self._stateful = True
        self._name = self.NAME + "_" + str(period) + "_src_" + inputtype + "_" + str(inputperiod)
        self._logger = du.getlogger(self._name) #??
        self.high_source_column = high_source_column
        self.low_source_column = low_source_column
        self.close_source_column = close_source_column
        self.open_source_column = open_source_column
		
        self.bol_bandwith = bol_bandwith
        self.perc_bb_column_name = perc_bb_column_name
        self.middle_band = str(middleband)+"_"+str(period)
        self.stddev=str(stddev)+"_"+str(period)
        self.stddev=du.get_column_name(self.stddev,inputtype,inputperiod,symbol,suffix)
        #self.middle_band = str(middle_band)+"_"+str(period)
        self.upper_band=str(upper_band)+"_"+str(period)
        self.lower_band=str(lower_band)+"_"+str(period)
        
        self.bb_column_name = bb_column_name +"_"+str(period)
        self.bb_slope_col= bb_slope_col
        self.normalised_column = str(normalised_column)
        self.normalised_slope = normalised_slope
        self.slopeperiod = slopeperiod
        
		#-->DT_Multiple setting support
        self.adjustcolumnnames(period,inputtype,inputperiod,symbol,suffix)
		#<---
	   
        customindargsdict=self.getcustomindicatorargsdict()
        self.customtilist= cti.getcustom_new(customindargsdict)
        self.fieldnames.extend(
            [self.middle_band, self.stddev, self.upper_band, self.lower_band, self.bb_column_name, self.perc_bb_column_name,
             self.bol_bandwith,self.normalised_column])
        cti.extendfieldnames(self.customtilist, self.fieldnames)
		
		
    #-->DT_Multiple setting support 
	#adjust column names as per settings
    def adjustcolumnnames(self,period,inputtype,inputperiod,symbol,suffix):
        self.bol_bandwith = du.get_column_name(self.bol_bandwith,inputtype,inputperiod,symbol,suffix)
        self.perc_bb_column_name = du.get_column_name(self.perc_bb_column_name,inputtype,inputperiod,symbol,suffix)
        self.middle_band = du.get_column_name(self.middle_band,inputtype,inputperiod,symbol,suffix)
        self.upper_band=du.get_column_name(self.upper_band,inputtype,inputperiod,symbol,suffix)
        self.lower_band=du.get_column_name(self.lower_band,inputtype,inputperiod,symbol,suffix)
        self.bb_column_name = du.get_column_name(self.bb_column_name,inputtype,inputperiod,symbol,suffix)
        self.bb_slope_col =du.get_column_name(self.bb_slope_col,inputtype,inputperiod,symbol,suffix)
        self.normalised_column =du.get_column_name(self.normalised_column,inputtype,inputperiod,symbol,suffix)
        self.normalised_slope =du.get_column_name(self.normalised_slope,inputtype,inputperiod,symbol,suffix)
		
    def getcustomindicatorargsdict(self):
        dict={}
        dict["slope"]=[(self.bol_bandwith,self.bb_slope_col,self.slopeperiod),(self.normalised_column,self.normalised_slope,self.slopeperiod)]
        return dict

    def adjustdatatypes(self):
        self._state.index = self._state['date_time']
        self._state[self.middle_band] = self._state[self.middle_band].astype(float)
        self._state[self.low_source_column] = self._state[self.low_source_column].astype(float)
        self._state[self.high_source_column] = self._state[self.high_source_column].astype(float)
        self._state[self.close_source_column] = self._state[self.close_source_column].astype(float)
        self._state[self.open_source_column] = self._state[self.open_source_column].astype(float)

        self._state[self.stddev] = self._state[self.stddev].astype(float)
        self._state[self.lower_band] = self._state[self.lower_band].astype(float)
        self._state[self.bb_column_name] = self._state[self.bb_column_name].astype(float)
        self._state[self.perc_bb_column_name] = self._state[self.perc_bb_column_name].astype(float)
        self._state[self.bol_bandwith] = self._state[self.bol_bandwith].astype(float)
        self._state[self.normalised_column] = self._state[self.normalised_column].astype(float)
        if "bb_slope" in self._state: self._state["bb_slope"] = self._state["bb_slope"].astype(float)
        self._saveintermediatestate(self._state)

    def setvaluesforrowbyindex(self, df, tdf, datetime):
        smavalue = self._smavalue(tdf, datetime)
        stdvalue = self._stdvalue(tdf, datetime, smavalue)
        df[self.middle_band][datetime] = smavalue
        df[self.stddev][datetime] = stdvalue
        df[self.upper_band][datetime] = smavalue + 2 * stdvalue
        df[self.lower_band][datetime] = smavalue - 2 * stdvalue
        df[self.bb_column_name][datetime] = 4 * stdvalue
        b_perc = (df[self.open_source_column][datetime] - df[self.lower_band][
            datetime]) / (df[self.upper_band][datetime] - df[self.lower_band][datetime])
        b_perc=b_perc*100
        df[self.perc_bb_column_name][datetime] =b_perc
        tdf[self.perc_bb_column_name][datetime] =b_perc
        df[self.bol_bandwith][datetime] = ((df[self.upper_band][datetime] - df[self.lower_band][datetime]) /
                                           df[self.middle_band][datetime]) * 100
        tdf[self.bol_bandwith][datetime]=df[self.bol_bandwith][datetime]
        df[self.normalised_column][datetime] = (df[self.bol_bandwith][datetime] *10000) /df[self.open_source_column][datetime]
        tdf[self.normalised_column][datetime]=df[self.normalised_column][datetime]
        
        cti.setvaluesforrowbyindex(self.customtilist, df, tdf, datetime)

    def _smavalue(self, tdf, datetime):
        rowno = tdf.index.get_loc(datetime)
        if rowno ==0: return np.NaN
        sma=tdf[self.middle_band].iloc[rowno]
        if np.isfinite(float(sma)): return sma
        subset = tdf[rowno - (self._period - 1):rowno + 1]
        currsmavalue = (subset[self.open_source_column].mean())
        tdf[self.middle_band].iloc[rowno] = currsmavalue
        return currsmavalue

    def _stdvalue(self, tdf, datetime, smavalue):
        rowno = tdf.index.get_loc(datetime)
        if rowno == 0: return np.NaN
        std = tdf[self.stddev].iloc[rowno]
        if np.isfinite(float(std)): return std
        subset = tdf[rowno - (self._period - 1):rowno + 1]
        currstdvalue = (subset[self.open_source_column].std())
        tdf[self.stddev].iloc[rowno] = currstdvalue
        return currstdvalue

    def _bbvalue(self, tdf, datetime, smavalue, stdvalue):
        bbvalue = tdf[self.stdev][datetime]
        if np.isfinite(float(bbvalue)): return bbvalue
        tdf[self.bb_column_name][datetime] = (tdf[self.upper_band][datetime] -tdf[self.lower_band][datetime])
        return tdf[self.bb_column_name][datetime]
	
	

    def _addtargetcolumns(self, df):
        if self.middle_band not in df: df[self.middle_band] = np.NaN
        if self.upper_band not in df: df[self.upper_band] = np.NaN
        if self.bb_column_name not in df: df[self.bb_column_name] = np.NaN
        if self.lower_band not in df: df[self.lower_band] = np.NaN
        if self.perc_bb_column_name not in df: df[self.perc_bb_column_name] = np.NaN
        if self.bol_bandwith not in df: df[self.bol_bandwith] = np.NaN
        if self.normalised_column not in df: df[self.normalised_column]=np.NaN
        if self.stddev not in df: df[self.stddev] = np.NaN
        df = cti._addtargetcolumns(self.customtilist, df)
        return df

    def _statelessindicator(self, df):
        subset = df[df[self.open_source_column].notnull()].head(self._period)
        datetime = subset['date_time'].iloc[self._period - 1]
        rowno = df.index.get_loc(datetime)
        sma = subset[self.open_source_column].mean()
        std = subset[self.open_source_column].std()
        df[self.middle_band].iloc[rowno] = sma
        df[self.stddev].iloc[rowno] = std
        df[self.upper_band] = sma + 2 *std
        df[self.lower_band] = sma - 2*std
        df[self.bb_column_name].iloc[rowno] = 4 * std
        df = cti._statelessindicator(self.customtilist, df)
        return self._statefulindicator(df)


    def _saveintermediatestate(self, df):
        if self._period >= int(self.slopeperiod) :
            self._state=self._state.append(df.tail(self._period))
            self._state = self._state.tail(self._period)
            #print('period is greater than slopeperiod')
        else:
            self._state=self._state.append(df.tail(int(self.slopeperiod)))
            self._state = self._state.tail(int(self.slopeperiod)) 
            #print('slopeperiod is greater than period')
        cti._saveintermediatestate(self.customtilist,df)
    def parameters(self):
        return str(self._period)+"_"+self.slopeperiod
	
	