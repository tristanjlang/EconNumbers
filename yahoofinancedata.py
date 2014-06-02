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
        elif element == '-' or element == '--' or element == '---' or element == 'nan' or element == 'Unch' or element == 'unch' or element == 'no change' or element == 'DELAYED' or element == 'DATE TBA': return NA
        elif element == 'ADP Employment' or element == 'ADP Employment Report': return 'ADP Employment Change'
        elif element == 'Case Shiller 20 City Index' or element == 'Case-Shiller 20 City' or element == 'Case-Shiller 20-city Index (y/y)' or element == 'Case-Shiller Housing Price Index' or element == 'CaseShiller 20 City' or element == 'CaseShiller Home Price Index' or element == 'S&P;/Case-Shiller Home Price Index' or element == 'S&P;/CaseShiller Composite' or element == 'S&P;/CaseShiller Home Price Index': return 'Case-Shiller 20-city Index'
        elif element == 'Core PCE Inflation' or element == 'PCE Prices' or element == 'Core PCE': return 'PCE Prices - Core'
        elif element == 'Current Account Balance': return 'Current Account'
        elif element == 'Durable Goods Orders': return 'Durable Orders'
        elif element == 'Durable Goods - Ex Transportation' or element == 'Durable Goods -ex Transportation' or element == 'Durable Orders - ex Transportation' or element == 'Durable Orders - ex transporation' or element == 'Durable Orders -ex Auto' or element == 'Durable Orders -ex Transporation' or element == 'Durable Orders -ex Transportation' or element == 'Durable Orders ex Auto' or element == 'Durable Orders ex Transporation' or element == 'Durable Orders ex Transportation' or element == 'Durable Orders ex auto' or element == 'Durable Orders ex transportation' or element == 'Durable Orders, Ex-Auto' or element == 'Durable Orders, Ex-Tran' or element == 'Durable Orders, Ex-Transportation' or element == 'Durable Ordes ex Transportation' or element == 'Durables, Ex Transportation' or element == 'Durables, Ex-Tran' or element == 'Durables, Ex-Transport' or element == 'Durables, Ex-Transportation' or element == 'Durables, ex Transporation': return 'Durable Goods -ex transportation'
        elif element == 'Net Long-term TIC Flows': return 'Net Long-Term TIC Flows'
        elif element == 'Trsy Budget': return 'Treasury Budget'
        elif element == 'Unit Labor Costs - Preliminary': return 'Unit Labor Costs -Prel'
        elif element == 'Nonfarm Payrolls - Private': return 'Nonfarm Private Payrolls'
        elif element == 'NAHB Market Housing Index': return 'NAHB Housing Market Index'
        elif element == 'Mich Sentiment-Rev': return 'Mich Sentiment-Rev.'
        elif element == 'FHFA Housing Price Index' or element == 'FHFA US Housing Price Index': return 'FHFA Home Price Index'
        elif element == 'NAPM Index': return 'ISM Index'
        elif element == 'NAPM Services': return 'ISM Services'
        elif element == 'NY Empire State Index': return 'Empire Manufacturing'
        else: return element
        '''
        -current account balance...some of them come up as on the order of 1e2 and most are on the order of 1e11
        -some of the items with % end up switching between % and no %....need to deal with this...
        '''
    def specialprocessrow(dfrow):
        global stats
        NAs = [NA, 'nan', 'unch', 'Unch', 'no change']

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
        
        '''if '%' in dfrow['Actual']:
            if dfrow['Statistic'] in stats:
                stats[dfrow['Statistic']] += 1
            else:
                stats[dfrow['Statistic']] = 1'''

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

    # remove the closes from the features
    X = X[['Actual', 'Briefing Forecast', 'Market Expects', 'Revised']]
    #X, y, y_adj = normalize(X), normalize(y), normalize(y_adj)
    X = X.applymap(abs)
    


    X['Statistic'] = merged['Statistic']
    X['Date'] = merged['Date']
    X['Year'] = merged['Year']
    return X, y, y_adj

    
#econdata(endyear=2002)
X, y, y_adj = processframe(pd.read_table('econdata.tsv'))
#print(X)
#print(y)
#print(y_adj)
group = X.groupby(['Statistic'])[['Actual', 'Briefing Forecast', 'Market Expects', 'Revised']]
for k, gp in group:
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

#print(X[X['Statistic'] == 'Mich Sentiment- Final'])