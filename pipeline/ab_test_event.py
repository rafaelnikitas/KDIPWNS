# -*- coding: utf-8 -*-
from libs import *
from config import config_dict
from etl import LoadJSONData,DataPipeline,AggCol

# function which will filter only event name transactions
'''Create function for filtering dataframe base on event key'''
def FilterEvent(df,event):
    df = df.copy()
    # apply filter
    df = df[df['event_key'] == event]
    return df

'''Create function for filtering out few July 2018 observations'''
def FilterOutMonth(df):
    df = df.copy()
    # apply filter
    df = df[df['date:month'] != 7]
    return df

'''Create function for filtering month'''
def FilterMonth(df,month):
    df = df.copy()
    # apply filter
    df = df[df['date:month'] == month]
    return df

'''Create a flag value for events'''
def FlagEvents(df):
    df = df.copy()
    # apply imputation
    df['events_flag'] = np.where((df['events'] > 1), 1 , df['events'])
    return df

'''Subset only certain columns for AB Testing'''
def ABTestSolution(df):
    df = df.copy()
    # select certain cols
    df = df[['session_id','event_name','variation_name','variation_binary','events_flag']].drop_duplicates()
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
    
'''Run flow for AB Testing each event seperately'''
def EventDF(df,event,month):
    df = df.copy()
    df = FilterEvent(df,event)
    if type(month) == int:
        df = FilterMonth(df,month)
    df = FlagEvents(df)
    df = ABTestSolution(df)
    return df
    
'''Create function to check distribution of event'''    
def PlotConversionRate(df, col=None, cont = None, binary=None, col_name = 'Variation', binary_name = None, dodge=True ):
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
    
'''AB Hypothesis Testing function'''    
def ABHypothesisTest(df,event,group,groupa,groupb):
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
    temp["Comment"] = np.where(temp["AB Hypothesis"] == "Fail to Reject H0", "Variations are similar!", "Variations are not similar!")
    
    return temp

'''Check every AB testing for event'''
def ABEvent(df,event):
    # create all possible tests
    df_ab_hp1 = ABHypothesisTest(df,event,'variation_binary','Original','Variation')
    df_ab_hp2 = ABHypothesisTest(df,event,'variation_name','Original','Variation #1')
    df_ab_hp3 = ABHypothesisTest(df,event,'variation_name','Original','Variation #2')
    df_ab_hp4 = ABHypothesisTest(df,event,'variation_name','Variation #1','Variation #2')
    # merge frames together
    frames = [df_ab_hp1,df_ab_hp2,df_ab_hp3,df_ab_hp4]
    df_ab_hp = pd.concat(frames)
    return df_ab_hp


'''Plot every possible event'''
def PlotABEvent(df,binary_name):
    # create all possible tests
    PlotConversionRate(df, col='variation_binary', binary='events_flag',binary_name=binary_name,dodge=True)
    PlotConversionRate(df, col='variation_name', binary='events_flag',binary_name=binary_name,dodge=True)
    return print('Plot')


'''Function to perform AB Testing'''   
def ABTestFlow(df,event,month,binary_name):
    df_ab = EventDF(df,event,month)
    PlotABEvent(df_ab,binary_name)
    df_ab_event = ABEvent(df_ab,event)
    return df_ab, df_ab_event

'''Main Function to perform AB Testing per event'''   
def ABTestEventMain():
       
       df = LoadJSONData(config_dict["PATH"],config_dict["FILE"])
       df = DataPipeline(df)
       df_checkout_ab, df_ab_all = ABTestFlow(df,config_dict["EVENT"],
                     config_dict["MONTH"],
                     config_dict["BINARY_NAME"])
       
       return df_ab_all
       
       
       
       
       
       
       
       
       

