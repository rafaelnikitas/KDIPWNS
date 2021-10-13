# -*- coding: utf-8 -*-
from libs import *
from config import config_dict
from etl import LoadJSONData,DataPipeline
from ab_test_event import PlotConversionRate,FilterEvent,FilterOutMonth,FlagEvents

'''Select certain event to plot conversion distribution'''
def EventDf(df,event):
    df = df.copy()
    df = FilterEvent(df,event)
    df = FilterOutMonth(df)
    df = FlagEvents(df)
    return df

'''Plot every possible date level event and extract valuable info'''
def PlotDateEvents(df):
    # plot different levels of date conversion event
    PlotConversionRate(df, col='date:month', binary='events_flag',binary_name='Month',dodge=True)
    PlotConversionRate(df, col='date:is_weekend', binary='events_flag',binary_name='Weekend',dodge=True)
    PlotConversionRate(df, col='date:day_of_week', binary='events_flag',binary_name='Day of Week',dodge=True)
    PlotConversionRate(df, col='date:day', binary='events_flag',binary_name='Day of Month',dodge=True)
    PlotConversionRate(df, col='date:day_of_year', binary='events_flag',binary_name='Day of Year',dodge=True)
    return print('Plot')

"""Put together filtering and plotting"""
def FilterPlotDateEvent(df,event):
    df = EventDf(df,event)
    PlotDateEvents(df)
    return print(event + " event is complete!")


'''Main Function to plot certain event per date'''   
def PlotEventDate():
       
       df = LoadJSONData(config_dict["PATH"],config_dict["FILE"])
       df = DataPipeline(df)
       FilterPlotDateEvent(df,config_dict["EVENT"])
       
       return print("Plot main run!")