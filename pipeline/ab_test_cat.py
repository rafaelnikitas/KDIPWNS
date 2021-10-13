# -*- coding: utf-8 -*-
from libs import *
from config import config_dict
from etl import LoadJSONData,DataPipeline,AggCol


'''Subset only certain columns for AB Testing'''
def ABTestSolutionCat(df,col):
    df = df.copy()
    # select certain cols
    df = df[['session_id','event_name',col,'events_flag']].drop_duplicates()
    # test if number of rows equals to number of unique session_ids
    test = len(df) == len(df['session_id'].drop_duplicates())
    print("Unique of ids is: ")
    print(test)
    # check number of duplicate ids 
    df['cnt'] = AggCol(df,'session_id','session_id','count')
    df_dupl = df[df['cnt'] > 1]
    # print number of ids 
    print("Duplicate number of ids is: ")
    print(len(df_dupl['session_id'].drop_duplicates()))
    return df


'''Check every AB testing for event'''
def ABEventTypeCat(df,event,col):
    # create all possible tests
    if col == 'visitType':
        df_ab_hp = ABHypothesisTestCat(df,event,col,'Returning Visit','New Visit')
    elif col == 'landingPage':
        df_ab_hp1 = ABHypothesisTestCat(df,event,col,'home','shop_details')
        df_ab_hp2 = ABHypothesisTestCat(df,event,col,'home','shop_list')
        df_ab_hp3 = ABHypothesisTestCat(df,event,col,'home','other')
        df_ab_hp4 = ABHypothesisTestCat(df,event,col,'shop_details','shop_list')
        df_ab_hp5 = ABHypothesisTestCat(df,event,col,'shop_details','other')
        df_ab_hp6 = ABHypothesisTestCat(df,event,col,'shop_list','other')
        frames = [df_ab_hp1,df_ab_hp2,df_ab_hp3,df_ab_hp4,df_ab_hp5,df_ab_hp6]
        df_ab_hp = pd.concat(frames)
    elif col == 'source':
        df_ab_hp1 = ABHypothesisTest(df,event,col,'google',"(direct)")
        df_ab_hp2 = ABHypothesisTest(df,event,col,'google','other')
        df_ab_hp3 = ABHypothesisTest(df,event,col,"(direct)",'other')   
        frames = [df_ab_hp1,df_ab_hp2,df_ab_hp3]
        df_ab_hp = pd.concat(frames)
    elif col == 'appBrowser':
        df_ab_hp1 = ABHypothesisTestCat(df,event,col,'Chrome','Firefox')
        df_ab_hp2 = ABHypothesisTestCat(df,event,col,'Chrome','other')
        df_ab_hp3 = ABHypothesisTestCat(df,event,col,'Firefox','other')   
        frames = [df_ab_hp1,df_ab_hp2,df_ab_hp3]
        df_ab_hp = pd.concat(frames)
    elif col == 'medium':
        df_ab_hp1 = ABHypothesisTestCat(df,event,col,'organic','cpc')
        df_ab_hp2 = ABHypothesisTestCat(df,event,col,'organic','other')
        df_ab_hp3 = ABHypothesisTestCat(df,event,col,'organic',"(none)")   
        df_ab_hp4 = ABHypothesisTestCat(df,event,col,'cpc',"(none)")
        df_ab_hp5 = ABHypothesisTestCat(df,event,col,'cpc','other')
        df_ab_hp6 = ABHypothesisTestCat(df,event,col,"(none)",'other')
        frames = [df_ab_hp1,df_ab_hp2,df_ab_hp3,df_ab_hp4,df_ab_hp5,df_ab_hp6]
        df_ab_hp = pd.concat(frames)

    return df_ab_hp


'''AB Hypothesis Testing function'''    
def ABHypothesisTestCat(df,event,group,groupa,groupb):
    # 
    from statsmodels.stats.proportion import proportions_ztest, proportion_confint
    control_results = df[df[group] == groupa]['events_flag']
    treatment_results = df[df[group] == groupb]['events_flag']
    n_con = control_results.count()
    n_treat = treatment_results.count()
    successes = [control_results.sum(), treatment_results.sum()]
    nobs = [n_con, n_treat]

    z_stat, pval = proportions_ztest(successes, nobs=nobs)
    (lower_con, lower_treat), (upper_con, upper_treat) = proportion_confint(successes, nobs=nobs, alpha=0.05)

    print(f'z statistic: {z_stat:.2f}')
    print(f'p-value: {pval:.3f}')
    print(f'ci 95% for control group: [{lower_con:.3f}, {upper_con:.3f}]')
    print(f'ci 95% for treatment group: [{lower_treat:.3f}, {upper_treat:.3f}]')
    
    # Result
    temp = pd.DataFrame({
        "Event":[event],
        "Group A":[groupa],
        "Group B":[groupb], 
        "p-value":[pval],
    })
    temp["AB Hypothesis"] = np.where(pval > 0.05, "Fail to Reject H0", "Reject H0")
    temp["Comment"] = np.where(temp["AB Hypothesis"] == "Fail to Reject H0", "Groups are similar!", "Groups are not similar!")
    
    return temp


'''Run flow for AB Testing each event seperately'''
def EventDFCat(df,event,month,col):
    df = df.copy()
    df = FilterEvent(df,event)
    if type(month) == int:
        df = FilterMonth(df,month)
    df = FlagEvents(df)
    df = ABTestSolutionCat(df,col)
    return df


'''Create function to check distribution of event'''    
def PlotConversionRateCat(df, col=None, cont = None, binary=None, col_name = 'Variation', binary_name = None, dodge=True ):
    total = len(df)
    tmp = pd.crosstab(df[col], df[binary], normalize='index') * 100
    tmp = tmp.reset_index()

    plt.figure(figsize=(16,12))

    plt.subplot(221)
    g= sns.countplot(x=col, data=df, order=list(tmp[col].values) , color='green')
    g.set_title(f'{binary_name} Distribution', 
                fontsize=20)
    g.set_xlabel(f'{binary_name} Values',fontsize=17)
    g.set_ylabel('Count Distribution', fontsize=17)
    sizes = []
    for p in g.patches:
        height = p.get_height()
        sizes.append(height)
        g.text(p.get_x()+p.get_width()/2.,
                height + 3,
                '{:1.0f}'.format(height),
                ha="center", fontsize=15) 
    g.set_ylim(0,max(sizes)*1.15)

    plt.subplot(222)
    g1= sns.countplot(x=col, data=df, order=list(tmp[col].values),
                     hue=binary,palette="hls")
    g1.set_title(f'{binary_name} Distribution by Conversion ratio %', 
                fontsize=20)
    gt = g1.twinx()
    gt = sns.pointplot(x=col, y=1, data=tmp, order=list(tmp[col].values),
                       color='black', legend=False)
    gt.set_ylim(0,tmp[1].max()*1.1)
    gt.set_ylabel("Conversion %Ratio", fontsize=16)
    g1.set_ylabel('Count Distribution',fontsize=17)
    g1.set_xlabel(f'{binary_name} Values', fontsize=17)
    
    sizes = []
    
    for p in g1.patches:
        height = p.get_height()
        sizes.append(height)
        g1.text(p.get_x()+p.get_width()/2.,
                height + 3,
                '{:1.2f}%'.format(height/total*100),
                ha="center", fontsize=10) 
    g1.set_ylim(0,max(sizes)*1.15)
    
    plt.show()

'''Plot every possible event'''
def PlotABEventCat(df,binary_name,col):
    # create all possible tests
    PlotConversionRate(df, col=col, binary='events_flag',binary_name=binary_name,dodge=True)
    return print('Plot')



'''Function to perform AB Testing'''   
def ABTestFlowCat(df,event,month,col,binary_name):
    df_ab = EventDFCat(df,event,month,col)
    PlotABEventCat(df_ab,binary_name,col)
    df_ab_event = ABEventTypeCat(df_ab,event,col)
    return df_ab, df_ab_event


'''Main Function to perform AB Testing per event'''   
def ABTestCatMain():
       
       df = LoadJSONData(config_dict["PATH"],config_dict["FILE"])
       df = DataPipeline(df)
       df_checkout_ab, df_ab_all = ABTestFlowCat(df,config_dict["EVENT"],
                     config_dict["MONTH"],
                     config_dict["COL"],
                     config_dict["BINARY_NAME"])
       
       return df_ab_all
















