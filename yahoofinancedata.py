'''
Yahoo Finance puts economic data in html at: http://biz.yahoo.com/c/ec/200101.html
URL format is: "<four digit year><week of year>.html"

It is in an html table, so just extract the data from the html:
1. economic data will be found in "soup.find_all('tr')", starting with element 6
2. the data can be extracted by doing a ".find_all('td')" on each tr element found
3. #2 will correspond with the following data:
    -Date
    -Time (ET)
    -Statistic
    -For (month or period being referenced)
    -Actual
    -Briefing Forecast
    -Market Expects
    -Prior
    -Revised
4. repeat for all weeks of the year 01 through 53 (if applicable)
5. repeat for all years from 2001 through 2014 (where applicable)
6. write to csv so pandas can read it
'''

from bs4 import BeautifulSoup
from urllib import request
import numpy as np
from numpy import nan as NA
import pandas as pd
import pandas.io.data as web
from pandas.tseries.offsets import BDay
from datetime import datetime

stats = {}
def econdata(startyear=2001, endyear=2015):
    f = open('econdata.tsv', 'w')
    f.write('\t'.join(['Year','Week','Date','Time (ET)','Statistic','For','Actual','Briefing Forecast','Market Expects','Prior','Revised\n']))

    for year in range(startyear, endyear):
        year = str(year)
        for week in range(1, 54):
            try:
                week = str(week) if week > 9 else '0' + str(week)
                r = request.urlopen('http://biz.yahoo.com/c/ec/' + year + week + '.html')
                soup = BeautifulSoup(r.readall())
                for tr in soup.find_all('tr')[6:]:
                    f.write('\t'.join([year, week] + [td.text for td in tr.find_all('td')]) + '\n')
            except: pass
    f.close()
    return pd.read_table('econdata.tsv')


def mktdata(startdate='1/1/2001', enddate='12/31/2014'):
    return web.get_data_yahoo('SPY', startdate, enddate) * 10


def processframe(econdf):
    NAs = [NA, 'nan', 'unch', 'Unch', 'no change', '-', '--', '---', 'DELAYED', 'DATE TBA', 'NA.']
    marketdata = mktdata()

    def processelement(element):
        if not isinstance(element, str) or ':' in element: return str(element)
        if len(element) > 2 and (element[0] == '$' or element[1] == '$'): element = element.replace('$','')
        if element[-1].upper() == 'K' and (element[-2].isdigit() or element[-3].isdigit()): return float(element[:-1]) * 1000
        elif element[-1].upper() == 'M' and (element[-2].isdigit() or element[-3].isdigit()): return float(element[:-1]) * 1000000
        elif element[-1].upper() == 'B' and (element[-2].isdigit() or element[-3].isdigit()): return float(element[:-1]) * 1000000000
        elif 'mln' in element: return float(element[:-3]) * 1000000
        elif 'bln' in element: return float(element[:-3]) * 1000000000
        elif element == '0.00%-0.25%' or element == '0-0.25%' or element == '0.00% -0.25%': return 0.25
        elif element[-2:] == '.%' or element[-2:] == '%%': return float(element[:-2])
        elif element[-1] == '%': return float(element[:-1]) if len(element) > 3 and element[-3] != ',' else float(element.replace(',','.')[:-1])
        elif element in NAs: return NA
        elif element in ['ADP Employment', 'ADP Employment Report']: return 'ADP Employment Change'
        elif element in ['Case Shiller 20 City Index', 'Case-Shiller 20 City', 'Case-Shiller 20-city Index (y/y)', 'Case-Shiller Housing Price Index', 'CaseShiller 20 City', 'CaseShiller Home Price Index', 'S&P;/Case-Shiller Home Price Index', 'S&P;/CaseShiller Composite', 'S&P;/CaseShiller Home Price Index']: return 'Case-Shiller 20-city Index'
        elif element in ['Core PCE Inflation', 'PCE Prices', 'Core PCE']: return 'PCE Prices - Core'
        elif element == 'Current Account Balance': return 'Current Account'
        elif element == 'Durable Goods Orders': return 'Durable Orders'
        elif element in ['Durable Goods - Ex Transportation', 'Durable Goods -ex Transportation', 'Durable Orders - ex Transportation', 'Durable Orders - ex transporation', 'Durable Orders -ex Auto', 'Durable Orders -ex Transporation', 'Durable Orders -ex Transportation', 'Durable Orders ex Auto', 'Durable Orders ex Transporation', 'Durable Orders ex Transportation', 'Durable Orders ex auto', 'Durable Orders ex transportation', 'Durable Orders, Ex-Auto', 'Durable Orders, Ex-Tran', 'Durable Orders, Ex-Transportation', 'Durable Ordes ex Transportation', 'Durables, Ex Transportation', 'Durables, Ex-Tran', 'Durables, Ex-Transport', 'Durables, Ex-Transportation', 'Durables, ex Transporation']: return 'Durable Goods -ex transportation'
        elif element == 'Net Long-term TIC Flows': return 'Net Long-Term TIC Flows'
        elif element == 'Trsy Budget': return 'Treasury Budget'
        elif element == 'Unit Labor Costs - Preliminary': return 'Unit Labor Costs -Prel'
        elif element == 'Nonfarm Payrolls - Private': return 'Nonfarm Private Payrolls'
        elif element == 'NAHB Market Housing Index': return 'NAHB Housing Market Index'
        elif element == 'Mich Sentiment-Rev': return 'Mich Sentiment-Rev.'
        elif element in ['FHFA Housing Price Index', 'FHFA US Housing Price Index']: return 'FHFA Home Price Index'
        elif element == 'NAPM Index': return 'ISM Index'
        elif element == 'NAPM Services': return 'ISM Services'
        elif element in ['NY Empire State Index', 'Empire Manufacturing Index']: return 'Empire Manufacturing'
        elif 'bcf' in element.lower(): return element[:-3]
        elif 'bp' in element.lower(): return float(element[:-2]) / 100
        else: return element
        
    def specialprocessrow(dfrow):
        if dfrow['Statistic'] in ['Trade Balance', 'Current Account', 'Current Account Balance', 'Consumer Credit', 'Treasury Budget', 'Trsy Budget']:
            if dfrow['Actual'] not in NAs and len(str(dfrow['Actual'])) > 1 and str(dfrow['Actual'])[-1] != 'B' and str(dfrow['Actual'])[-1] != 'M' and str(dfrow['Actual'])[-1] != '-': dfrow['Actual'] = str(dfrow['Actual']) + 'B'
            if dfrow['Briefing Forecast'] not in NAs and len(str(dfrow['Briefing Forecast'])) > 1 and str(dfrow['Briefing Forecast'])[-1] != 'B' and str(dfrow['Briefing Forecast'])[-1] != 'M' and str(dfrow['Briefing Forecast'])[-1] != '-': dfrow['Briefing Forecast'] = str(dfrow['Briefing Forecast']) + 'B'
            if dfrow['Market Expects'] not in NAs and len(str(dfrow['Market Expects'])) > 1 and str(dfrow['Market Expects'])[-1] != 'B' and str(dfrow['Market Expects'])[-1] != 'M' and str(dfrow['Market Expects'])[-1] != '-': dfrow['Market Expects'] = str(dfrow['Market Expects']) + 'B'
            if dfrow['Revised'] not in NAs and len(str(dfrow['Revised'])) > 1 and str(dfrow['Revised'])[-1] != 'B' and str(dfrow['Revised'])[-1] != 'M' and str(dfrow['Revised'])[-1] != '-': dfrow['Revised'] = str(dfrow['Revised']) + 'B'

        if dfrow['Statistic'] in ['Initial Claims', 'Housing Starts', 'New Home Sales']:
            if dfrow['Actual'] not in NAs and len(str(dfrow['Actual'])) > 1 and str(dfrow['Actual']).upper()[-1] != 'K' and str(dfrow['Actual']).upper()[-1] != 'M' and str(dfrow['Actual'])[-1] != '-': dfrow['Actual'] = str(dfrow['Actual']) + 'K'
            if dfrow['Briefing Forecast'] not in NAs and len(str(dfrow['Briefing Forecast'])) > 1 and str(dfrow['Briefing Forecast']).upper()[-1] != 'K' and str(dfrow['Briefing Forecast']).upper()[-1] != 'M' and str(dfrow['Briefing Forecast'])[-1] != '-': dfrow['Briefing Forecast'] = str(dfrow['Briefing Forecast']) + 'K'
            if dfrow['Market Expects'] not in NAs and len(str(dfrow['Market Expects'])) > 1 and str(dfrow['Market Expects']).upper()[-1] != 'K' and str(dfrow['Market Expects']).upper()[-1] != 'M' and str(dfrow['Market Expects'])[-1] != '-': dfrow['Market Expects'] = str(dfrow['Market Expects']) + 'K'
            if dfrow['Revised'] not in NAs and len(str(dfrow['Revised'])) > 1 and str(dfrow['Revised']).upper()[-1] != 'K' and str(dfrow['Revised']).upper()[-1] != 'M' and str(dfrow['Revised'])[-1] != '-': dfrow['Revised'] = str(dfrow['Revised']) + 'K'

        if dfrow['Statistic'] in ['Retail Sales']:
            try:
                if abs(float(dfrow['Actual'])) >= 100: dfrow['Actual'] = float(dfrow['Actual']) / 100
                if abs(float(dfrow['Briefing Forecast'])) >= 100: dfrow['Briefing Forecast'] = float(dfrow['Briefing Forecast']) / 100
                if abs(float(dfrow['Market Expects'])) >= 100: dfrow['Market Expects'] = float(dfrow['Market Expects']) / 100
                if abs(float(dfrow['Revised'])) >= 100: dfrow['Revised'] = float(dfrow['Revised']) / 100
            except: pass

        if dfrow['Statistic'] == 'Continuing Claims' and dfrow['Actual'] == '3.698K': dfrow['Actual'] = '3698K'
        if dfrow['Statistic'] == 'Building Permits' and dfrow['Actual'] == '1.669K': dfrow['Actual'] = '1.669M'
        
        return dfrow

    # *** MAJOR ASSUMPTION ***
    # if one of briefing forecast or market expects is NA, fill value with the other
    def myfillna(dfrow):
        if dfrow['Briefing Forecast'] in NAs and dfrow['Market Expects'] in NAs: return dfrow
        if dfrow['Briefing Forecast'] in NAs: dfrow['Briefing Forecast'] = float(dfrow['Market Expects'])
        if dfrow['Market Expects'] in NAs: dfrow['Market Expects'] = float(dfrow['Briefing Forecast'])
        return dfrow

    # additional helper functions
    '''
    NEED TO UPDATE NORMALIZE TO HANDLE NORMALIZE EACH COLUMN
    '''
    normalize = lambda df: (df - df.mean()) / (df.max() - df.min())
    getclosebeforedate = lambda dfrow: datetime.strptime(dfrow['Date'] + ' ' + dfrow['Year'], '%b %d %Y') - BDay(1) if dfrow['Time (ET)'][-2:] == 'AM' else datetime.strptime(dfrow['Date'] + ' ' + dfrow['Year'], '%b %d %Y')
        # close should refer to yesterday's date otherwise refer to date of event
    
    # apply helper functions
    df = econdf.apply(specialprocessrow, axis=1)
    df = df.applymap(processelement)
    df = df.apply(myfillna, axis=1)
    df['close date before event'] = df.apply(getclosebeforedate, axis=1)
    
    # convert strings that were not converted over to floats
    numdf = df[['Actual', 'Briefing Forecast', 'Market Expects', 'Revised', 'close date before event']]
    numdf = numdf.applymap(lambda x: float(x) if isinstance(x, str) else x)
    numdf[['Statistic', 'Date', 'Year']] = df[['Statistic', 'Date', 'Year']]

    # convert inputs for the Statistics to be percent change from briefing forecast or market expects to actual
    numdf['Pct Diff From Briefing Forecast'] = (numdf['Actual'] - numdf['Briefing Forecast']) / numdf['Briefing Forecast']
    numdf['Pct Diff From Market Expects'] = (numdf['Actual'] - numdf['Market Expects']) / numdf['Market Expects']

    # remove the closes and non-percent-change features from the features
    df_BF = numdf.copy()[['Pct Diff From Briefing Forecast', 'Statistic', 'Date', 'Year', 'close date before event']]
    df_ME = numdf.copy()[['Pct Diff From Market Expects', 'Statistic', 'Date', 'Year', 'close date before event']]
    df_BF = df_BF.replace([np.inf, -np.inf], np.nan).dropna()
    df_ME = df_ME.replace([np.inf, -np.inf], np.nan).dropna()

    # create list for the statistics, initialize them in the X df's to be zero
    statistics = [k for k, gp in numdf.groupby(['Statistic'])]
    X_BF = pd.DataFrame(columns=statistics, index=marketdata.index)
    X_ME = pd.DataFrame(columns=statistics, index=marketdata.index)

    # group by close date before event and loop through groups to enter statistic values
    group_BF = df_BF.groupby(['close date before event'])
    group_ME = df_ME.groupby(['close date before event'])

    for date, gp in group_BF:
        for statistic in gp['Statistic']:
            X_BF.ix[date, statistic] = [val for val in gp[gp['Statistic'] == statistic]['Pct Diff From Briefing Forecast']][0]

    for date, gp in group_ME:
        for statistic in gp['Statistic']:
            X_ME.ix[date, statistic] = [val for val in gp[gp['Statistic'] == statistic]['Pct Diff From Market Expects']][0]
    
    # drop rows that are all NA (no statistic entered for those dates)
    X_BF = X_BF.dropna(axis=1, how='all')
    X_ME = X_ME.dropna(axis=1, how='all')

    # fill in zeros for NA values
    '''
    MAY NEED TO FILL THESE AFTER DOING NORMALIZATION
    '''
    X_BF = X_BF.fillna(value=0)
    X_ME = X_ME.fillna(value=0)

    # add open date after event
    getopenafterdate = lambda dfrow: dfrow['close date before event'] + BDay(1)
    y_BF = pd.DataFrame(columns=['close date before event', 'open date after event'], index=X_BF.index)
    y_ME = pd.DataFrame(columns=['close date before event', 'open date after event'], index=X_ME.index)
    y_BF['close date before event'] = X_BF.index
    y_ME['close date before event'] = X_ME.index
    y_BF['open date after event'] = y_BF.apply(getopenafterdate, axis=1)
    y_ME['open date after event'] = y_ME.apply(getopenafterdate, axis=1)


    # merge market data with dates to get pertinent data for close before and open after for all the events of interest
    y_merged_BF = pd.merge(y_BF, marketdata, left_on='close date before event', right_index=True)
    y_merged_BF.columns = ['close date before event', 'open date after event', 'Open_before', 'High_before', 'Low_before', 'Close_before', 'Volume_before', 'Adj Close_before']
    y_merged_BF = pd.merge(y_merged_BF, marketdata, left_on='open date after event', right_index=True)
    y_merged_BF.columns = ['close date before event', 'open date after event', 'Open_before', 'High_before', 'Low_before', 'Close_before', 'Volume_before', 'Adj Close_before', 'Open_after', 'High_after', 'Low_after', 'Close_after', 'Volume_after', 'Adj Close_after']
    
    y_merged_ME = pd.merge(y_ME, marketdata, left_on='close date before event', right_index=True)
    y_merged_ME.columns = ['close date before event', 'open date after event', 'Open_before', 'High_before', 'Low_before', 'Close_before', 'Volume_before', 'Adj Close_before']
    y_merged_ME = pd.merge(y_merged_ME, marketdata, left_on='open date after event', right_index=True)
    y_merged_ME.columns = ['close date before event', 'open date after event', 'Open_before', 'High_before', 'Low_before', 'Close_before', 'Volume_before', 'Adj Close_before', 'Open_after', 'High_after', 'Low_after', 'Close_after', 'Volume_after', 'Adj Close_after']
    #merged = merged[merged['Market Expects'] != 'nan'].dropna()
    #merged = merged[merged['Market Expects'] != 'nan'].dropna()

    # keep only market numbers
    y_BF = y_merged_BF[['Close_before', 'Adj Close_before', 'Open_after', 'High_after', 'Low_after', 'Close_after', 'Adj Close_after']]
    y_ME = y_merged_ME[['Close_before', 'Adj Close_before', 'Open_after', 'High_after', 'Low_after', 'Close_after', 'Adj Close_after']]
    
    # separate out the output values based on the close or the adjusted close before the event
    y_ME_adj, y_BF_adj = y_ME.copy(), y_BF.copy()
    y_ME.is_copy, y_BF.is_copy = False, False

    # convert nominal opens/closes after the event to returns on the close before the event
    y_BF['Open_after'] = y_BF['Open_after'] / y_BF['Close_before']
    y_BF['High_after'] = y_BF['High_after'] / y_BF['Close_before']
    y_BF['Low_after'] = y_BF['Low_after'] / y_BF['Close_before']
    y_BF['Close_after'] = y_BF['Close_after'] / y_BF['Close_before']
    y_BF['Adj Close_after'] = y_BF['Adj Close_after'] / y_BF['Close_before']
    y_BF = y_BF[['Open_after', 'High_after', 'Low_after', 'Close_after', 'Adj Close_after']]
    y_BF.columns = ['r_Open_after', 'r_High_after', 'r_Low_after', 'r_Close_after', 'r_Adj Close_after']
    
    y_ME['Open_after'] = y_ME['Open_after'] / y_ME['Close_before']
    y_ME['High_after'] = y_ME['High_after'] / y_ME['Close_before']
    y_ME['Low_after'] = y_ME['Low_after'] / y_ME['Close_before']
    y_ME['Close_after'] = y_ME['Close_after'] / y_ME['Close_before']
    y_ME['Adj Close_after'] = y_ME['Adj Close_after'] / y_ME['Close_before']
    y_ME = y_ME[['Open_after', 'High_after', 'Low_after', 'Close_after', 'Adj Close_after']]
    y_ME.columns = ['r_Open_after', 'r_High_after', 'r_Low_after', 'r_Close_after', 'r_Adj Close_after']

    y_BF_adj['Open_after'] = y_BF_adj['Open_after'] / y_BF_adj['Adj Close_before']
    y_BF_adj['High_after'] = y_BF_adj['High_after'] / y_BF_adj['Adj Close_before']
    y_BF_adj['Low_after'] = y_BF_adj['Low_after'] / y_BF_adj['Adj Close_before']
    y_BF_adj['Close_after'] = y_BF_adj['Close_after'] / y_BF_adj['Adj Close_before']
    y_BF_adj['Adj Close_after'] = y_BF_adj['Adj Close_after'] / y_BF_adj['Adj Close_before']
    y_BF_adj = y_BF_adj[['Open_after', 'High_after', 'Low_after', 'Close_after', 'Adj Close_after']]
    y_BF_adj.columns = ['r_Open_after', 'r_High_after', 'r_Low_after', 'r_Close_after', 'r_Adj Close_after']

    y_ME_adj['Open_after'] = y_ME_adj['Open_after'] / y_ME_adj['Adj Close_before']
    y_ME_adj['High_after'] = y_ME_adj['High_after'] / y_ME_adj['Adj Close_before']
    y_ME_adj['Low_after'] = y_ME_adj['Low_after'] / y_ME_adj['Adj Close_before']
    y_ME_adj['Close_after'] = y_ME_adj['Close_after'] / y_ME_adj['Adj Close_before']
    y_ME_adj['Adj Close_after'] = y_ME_adj['Adj Close_after'] / y_ME_adj['Adj Close_before']
    y_ME_adj = y_ME_adj[['Open_after', 'High_after', 'Low_after', 'Close_after', 'Adj Close_after']]
    y_ME_adj.columns = ['r_Open_after', 'r_High_after', 'r_Low_after', 'r_Close_after', 'r_Adj Close_after']
    
    #X, y, y_adj = normalize(X), normalize(y), normalize(y_adj)
    #X = X.applymap(abs)
    
    return X_BF, X_ME#, y, y_adj

    
#econdata(endyear=2002)
#X_brief, X_mkt, y, y_adj = processframe(pd.read_table('econdata.tsv'))
X_brief, X_mkt = processframe(pd.read_table('econdata.tsv'))
#print(X_mkt)
#print(X)
#print(y)
#print(y_adj)
#group = X_brief.groupby(['Statistic'])[['Pct Diff From Briefing Forecast', 'Pct Diff From Market Expects']]
'''for k, gp in group:
    #print(k)
    print('max = ' + str(gp.max()) + '\n')
    print('min = ' + str(gp.min()) + '\n')
    print('mean = ' + str(gp.mean()) + '\n')
    print('median = ' + str(gp.median()) + '\n')
    print('\n\n\n')'''
#print(group.value_counts())
'''for k, v in sorted(stats.items()):
    print(k, v)
X = X[(X['Statistic'] == 'Retail Sales') & (('%' not in X['Actual']) | ('%' not in X['Briefing Forecast']) | ('%' not in X['Market Expects']) | ('%' not in X['Revised']))]
#X = X[(X['Statistic'] == 'Retail Sales') & (('%' in X['Actual']) | ('%' in X['Briefing Forecast']) | ('%' in X['Market Expects']) | ('%' in X['Revised']))]
print(X)'''

#print(X[X['Statistic'] == 'Help-Wanted Index'])