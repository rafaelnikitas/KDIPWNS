# -*- coding: utf-8 -*-

from ab_test_event import ABTestEventMain
from ab_test_cat import ABTestCatMain
from plot_dates import PlotEventDate
# process
event = True
plot = True

if event:
    ab_df = ABTestEventMain()
    print(ab_df)
else:
    ab_df = ABTestCatMain()
    print(ab_df)
    
if plot:
       PlotEventDate()
else:
       print("error")



