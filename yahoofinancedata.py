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
    getopenafterdate = lambda dfrow: dfrow['close date before event'] + BDay(1)
    getclosebeforedate = lambda dfrow: datetime.strptime(dfrow['Date'] + ' ' + dfrow['Year'], '%b %d %Y') - BDay(1) if dfrow['Time (ET)'][-2:] == 'AM' else datetime.strptime(dfrow['Date'] + ' ' + dfrow['Year'], '%b %d %Y')
        # close should refer to yesterday's date otherwise refer to date of event
    
    # apply helper functions
    df = econdf.apply(specialprocessrow, axis=1)
    df = df.applymap(processelement)
    df = df.apply(myfillna, axis=1)
    df['close date before event'] = df.apply(getclosebeforedate, axis=1)
    df['open date after event'] = df.apply(getopenafterdate, axis=1)
    
    # merge market data with econ data
    merged = pd.merge(df, mktdata(), left_on='close date before event', right_index=True)
    merged.columns = ['Year', 'Week', 'Date', 'Time (ET)', 'Statistic', 'For', 'Actual', 'Briefing Forecast', 'Market Expects', 'Prior', 'Revised', 'close date before event', 'open date after event', 'Open_before', 'High_before', 'Low_before', 'Close_before', 'Volume_before', 'Adj Close_before']
    merged = pd.merge(merged, mktdata(), left_on='open date after event', right_index=True)
    merged.columns = ['Year', 'Week', 'Date', 'Time (ET)', 'Statistic', 'For', 'Actual', 'Briefing Forecast', 'Market Expects', 'Prior', 'Revised', 'close date before event', 'open date after event', 'Open_before', 'High_before', 'Low_before', 'Close_before', 'Volume_before', 'Adj Close_before', 'Open_after', 'High_after', 'Low_after', 'Close_after', 'Volume_after', 'Adj Close_after']
    merged = merged[merged['Market Expects'] != 'nan'].dropna()

    # change to X and y and remove text-based features
    X = merged[['Actual', 'Briefing Forecast', 'Market Expects', 'Revised', 'Close_before', 'Adj Close_before']]
    y = merged[['Open_after', 'High_after', 'Low_after', 'Close_after', 'Adj Close_after']]
    X = X.applymap(lambda x: float(x) if isinstance(x, str) else x)
    X['Statistic'] = merged['Statistic']
    X['Date'] = merged['Date']
    X['Year'] = merged['Year']
    
    # separate out the output values based on the close or the adjusted close before the event
    y_adj = y.copy()
    y.is_copy = False

    # convert nominal opens/closes after the event to returns on the close before the event
    y['Open_after'] = y['Open_after'] / X['Close_before']
    y['High_after'] = y['High_after'] / X['Close_before']
    y['Low_after'] = y['Low_after'] / X['Close_before']
    y['Close_after'] = y['Close_after'] / X['Close_before']
    y['Adj Close_after'] = y['Adj Close_after'] / X['Close_before']
    y.columns = ['r_Open_after', 'r_High_after', 'r_Low_after', 'r_Close_after', 'r_Adj Close_after']
    
    y_adj['Open_after'] = y_adj['Open_after'] / X['Adj Close_before']
    y_adj['High_after'] = y_adj['High_after'] / X['Adj Close_before']
    y_adj['Low_after'] = y_adj['Low_after'] / X['Adj Close_before']
    y_adj['Close_after'] = y_adj['Close_after'] / X['Adj Close_before']
    y_adj['Adj Close_after'] = y_adj['Adj Close_after'] / X['Adj Close_before']
    y_adj.columns = ['r_Open_after', 'r_High_after', 'r_Low_after', 'r_Close_after', 'r_Adj Close_after']

    # convert inputs for the Statistics to be percent change from briefing forecast or market expects to actual
    X['Pct Diff From Briefing Forecast'] = (X['Actual'] - X['Briefing Forecast']) / X['Briefing Forecast']
    X['Pct Diff From Market Expects'] = (X['Actual'] - X['Market Expects']) / X['Market Expects']

    # remove the closes and non-percent-change features from the features
    X_brief = X[['Pct Diff From Briefing Forecast', 'Statistic', 'Date', 'Year']]
    X_mkt = X[['Pct Diff From Market Expects', 'Statistic', 'Date', 'Year']]
    X_brief = X_brief.replace([np.inf, -np.inf], np.nan).dropna()
    X_mkt = X_mkt.replace([np.inf, -np.inf], np.nan).dropna()

    #X, y, y_adj = normalize(X), normalize(y), normalize(y_adj)
    #X = X.applymap(abs)
    
    return X_brief, X_mkt, y, y_adj

    
#econdata(endyear=2002)
X_brief, X_mkt, y, y_adj = processframe(pd.read_table('econdata.tsv'))
#print(X)
#print(y)
#print(y_adj)
group = X_brief.groupby(['Statistic'])[['Pct Diff From Briefing Forecast', 'Pct Diff From Market Expects']]
for k, gp in group:
    #print(k)
    print('max = ' + str(gp.max()) + '\n')
    print('min = ' + str(gp.min()) + '\n')
    print('mean = ' + str(gp.mean()) + '\n')
    print('median = ' + str(gp.median()) + '\n')
    print('\n\n\n')
#print(group.value_counts())
'''for k, v in sorted(stats.items()):
    print(k, v)
X = X[(X['Statistic'] == 'Retail Sales') & (('%' not in X['Actual']) | ('%' not in X['Briefing Forecast']) | ('%' not in X['Market Expects']) | ('%' not in X['Revised']))]
#X = X[(X['Statistic'] == 'Retail Sales') & (('%' in X['Actual']) | ('%' in X['Briefing Forecast']) | ('%' in X['Market Expects']) | ('%' in X['Revised']))]
print(X)'''

#print(X[X['Statistic'] == 'Help-Wanted Index'])




##




'''
FULL LIST OF FEATURES TO ADD AS COLUMNS:


ADP Employment Change
Auto Sales
Average Workweek
Building Permits
Business Inventories
CPI
Capacity Utilization
Case-Shiller 20-city Index
Chain Deflator-Adv.
Chain Deflator-Final
Chain Deflator-Prel.
Chicago PMI
Construction Spending
Consumer Confidence
Consumer Credit
Continuing Claims
Core CPI
Core PPI
Current Account
Durable Goods -ex transportation
Durable Orders
Empire Manufacturing
Employment Cost Index
Existing Home Sales
FHFA Home Price Index
Factory Orders
GDP Deflator
GDP-Adv.
GDP-Final
GDP-Prel.
Help-Wanted Index
Hourly Earnings
Housing Starts
ISM Index
ISM Services
Industrial Production
Initial Claims
Leading Indicators
Mich Sentiment- Final
Mich Sentiment-Prel.
Mich Sentiment-Rev.
NAHB Housing Market Index
Net Foreign Purchases
Net Long-Term TIC Flows
New Home Sales
Nonfarm Payrolls
Nonfarm Private Payrolls
PCE Prices - Core
PPI
Pending Home Sales
Personal Income
Personal Spending
Philadelphia Fed
Productivity-Prel
Productivity-Rev.
Retail Sales
Retail Sales ex-auto
Trade Balance
Treasury Budget
Truck Sales
Unemployment Rate
Unit Labor Costs
Unit Labor Costs - Rev
Unit Labor Costs -Prel
Wholesale Inventories'''