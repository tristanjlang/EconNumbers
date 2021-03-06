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
from sklearn.linear_model import LinearRegression
from sklearn.linear_model import Ridge
from sklearn.linear_model import Lasso
from sklearn.cross_validation import train_test_split
from scipy.sparse import csr_matrix


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
    today = datetime.today()

    def bdayoffset(df, date, increment=True):
        n = 1
        while date + BDay(n) < today:
            if increment:
                if date + BDay(n) not in df.index: n += 1
                else: return date + BDay(n)
            else:
                if date - BDay(n) not in df.index: n -= 1
                else: return date - BDay(n)
        return date + BDay(n)

    # helper functions
    getopenafterdate = lambda dfrow: bdayoffset(marketdata, dfrow['close date before event'])
    normalize = lambda dfcol: (dfcol - dfcol.mean()) / (dfcol.max() - dfcol.min()) if dfcol.max() != dfcol.min() else NA
    getclosebeforedate = lambda dfrow: bdayoffset(marketdata, datetime.strptime(dfrow['Date'] + ' ' + dfrow['Year'], '%b %d %Y'), False) if dfrow['Time (ET)'][-2:] == 'AM' else datetime.strptime(dfrow['Date'] + ' ' + dfrow['Year'], '%b %d %Y')
        # close should refer to yesterday's date otherwise refer to date of event

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
        elif element in ['Core PCE Inflation', 'PCE Prices', 'Core PCE', 'PCE Core Inflation', 'PCE Core', 'Core PCE Prices']: return 'PCE Prices - Core'
        elif element == 'Current Account Balance': return 'Current Account'
        elif element == 'Durable Goods Orders': return 'Durable Orders'
        elif element in ['Durable Goods - Ex Transportation', 'Durable Goods -ex Transportation', 'Durable Orders - ex Transportation', 'Durable Orders - ex transporation', 'Durable Orders -ex Auto', 'Durable Orders -ex Transporation', 'Durable Orders -ex Transportation', 'Durable Orders ex Auto', 'Durable Orders ex Transporation', 'Durable Orders ex Transportation', 'Durable Orders ex auto', 'Durable Orders ex transportation', 'Durable Orders, Ex-Auto', 'Durable Orders, Ex-Tran', 'Durable Orders, Ex-Transportation', 'Durable Ordes ex Transportation', 'Durables, Ex Transportation', 'Durables, Ex-Tran', 'Durables, Ex-Transport', 'Durables, Ex-Transportation', 'Durables, ex Transporation', 'Durable Goods Orders - ex Transporation', 'Durable Goods Orders -ex Transportation', 'Durable Goods Orders ex Auto', 'Durable Orders -ex transportation']: return 'Durable Goods -ex transportation'
        elif element == 'Net Long-term TIC Flows': return 'Net Long-Term TIC Flows'
        elif element == 'Trsy Budget': return 'Treasury Budget'
        elif element == 'Unit Labor Costs - Preliminary': return 'Unit Labor Costs -Prel'
        elif element in ['Unit Labor Costs - Rev', 'Unit Labor Costs - Rev.', 'Unit Labor Costs - Revised', 'Unit Labor Costs-Rev', 'Unit Labor Costs-Rev.']: return 'Unit Labor Costs -Rev'
        elif element in ['University of Michigan Sentiment - Final', 'U. Michigan Consumer Sentiment', 'University of Michigan Sentiment', 'U Michigan Sentiment - Final', 'Michigan Sentiment - Final', 'Mich Sentiment- Final', 'Mich Sentiment']: return 'U Michigan Consumer Sentiment - Final'
        elif element in ['Mich Sentiment-Prel.', 'Mich Sentiment-Prel']: return 'Mich Sentiment - Preliminary'
        elif element == 'Nonfarm Payrolls - Private': return 'Nonfarm Private Payrolls'
        elif element in ['NAHB Market Housing Index', 'NAHB Market Index']: return 'NAHB Housing Market Index'
        elif element == 'Mich Sentiment-Rev': return 'Mich Sentiment-Rev.'
        elif element in ['FHFA Housing Price Index', 'FHFA US Housing Price Index']: return 'FHFA Home Price Index'
        elif element == 'NAPM Index': return 'ISM Index'
        elif element == 'NAPM Services': return 'ISM Services'
        elif element in ['GDP - Deflator', 'GDP Deflator', 'GDP Deflator - Second Estimate', 'GDP Deflator - Third Estimate']: return 'Chain Deflator-Final'
        elif element in ['NY Empire State Index', 'Empire Manufacturing Index', 'Empire Manufacturing Survey', 'Empire State Mfg.', 'NY Empire Manufacturing Index', 'NY Fed - Empire Manufacturing Index', 'NY Fed - Empire Manufacturing Survey']: return 'Empire Manufacturing'
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

    def generate_input_output(statistics, marketdata, numdf, type_is_briefing_forecast=True):
        stat_type = 'Pct Diff From Briefing Forecast' if type_is_briefing_forecast else 'Pct Diff From Market Expects'

        # remove the closes and non-percent-change features from the features
        df = numdf.copy()[[stat_type, 'Statistic', 'Date', 'Year', 'close date before event']]
        df = df.replace([np.inf, -np.inf], np.nan).dropna()
        
        # initialize statistics in the X df's to be zero
        X = pd.DataFrame(columns=statistics, index=marketdata.index)

        # group by close date before event and loop through groups to enter statistic values
        group = df.groupby(['close date before event'])
    
        for date, gp in group:
            for statistic in gp['Statistic']:
                X.ix[date, statistic] = [val for val in gp[gp['Statistic'] == statistic][stat_type]][0]

        # drop rows that are all NA (no statistic entered for those dates)
        X = X.dropna(axis=1, how='all')

        # normalize, then fill in zeros for NA values
        X = X.apply(normalize)
        X = X.fillna(value=0)

        # add open date after event
        y = pd.DataFrame(columns=['close date before event', 'open date after event'], index=X.index)
        y['close date before event'] = X.index
        y['open date after event'] = y.apply(getopenafterdate, axis=1)

        # merge market data with dates to get pertinent data for close before and open after for all the events of interest
        y_merged = pd.merge(y, marketdata, left_on='close date before event', right_index=True)
        y_merged.columns = ['close date before event', 'open date after event', 'Open_before', 'High_before', 'Low_before', 'Close_before', 'Volume_before', 'Adj Close_before']
        y_merged = pd.merge(y_merged, marketdata, left_on='open date after event', right_index=True)
        y_merged.columns = ['close date before event', 'open date after event', 'Open_before', 'High_before', 'Low_before', 'Close_before', 'Volume_before', 'Adj Close_before', 'Open_after', 'High_after', 'Low_after', 'Close_after', 'Volume_after', 'Adj Close_after']
        
        # keep only market numbers
        y = y_merged[['Close_before', 'Adj Close_before', 'Open_after', 'High_after', 'Low_after', 'Close_after', 'Adj Close_after']]
        
        # separate out the output values based on the close or the adjusted close before the event
        y_adj = y.copy()
        y.is_copy = False

        # convert nominal opens/closes after the event to returns on the close before the event
        y['Open_after'] = y['Open_after'] / y['Close_before']
        y['High_after'] = y['High_after'] / y['Close_before']
        y['Low_after'] = y['Low_after'] / y['Close_before']
        y['Close_after'] = y['Close_after'] / y['Close_before']
        y['Adj Close_after'] = y['Adj Close_after'] / y['Close_before']
        y = y[['Open_after', 'High_after', 'Low_after', 'Close_after', 'Adj Close_after']]
        y.columns = ['r_Open_after', 'r_High_after', 'r_Low_after', 'r_Close_after', 'r_Adj Close_after']
        
        y_adj['Open_after'] = y_adj['Open_after'] / y_adj['Adj Close_before']
        y_adj['High_after'] = y_adj['High_after'] / y_adj['Adj Close_before']
        y_adj['Low_after'] = y_adj['Low_after'] / y_adj['Adj Close_before']
        y_adj['Close_after'] = y_adj['Close_after'] / y_adj['Adj Close_before']
        y_adj['Adj Close_after'] = y_adj['Adj Close_after'] / y_adj['Adj Close_before']
        y_adj = y_adj[['Open_after', 'High_after', 'Low_after', 'Close_after', 'Adj Close_after']]
        y_adj.columns = ['r_Open_after', 'r_High_after', 'r_Low_after', 'r_Close_after', 'r_Adj Close_after']

        y, y_adj = y.apply(normalize), y_adj.apply(normalize)
        return X, y, y_adj

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
    statistics = [k for k, gp in numdf.groupby(['Statistic'])]
    #X = X.applymap(abs)

    X_BF, y_BF, y_BF_adj = generate_input_output(statistics, marketdata, numdf, True)
    X_ME, y_ME, y_ME_adj = generate_input_output(statistics, marketdata, numdf, False)
    
    return X_BF, y_BF, y_BF_adj, X_ME, y_ME, y_ME_adj

    
# returns training data set, cross validation data set, testing data set
def training_validation_testing_sets(df):
    trainingsize = int(len(df) * 0.6)
    validationsize = int(len(df) * 0.2) + trainingsize
    testingsize = len(df) - validationsize
    return df.ix[:trainingsize], df.ix[trainingsize : validationsize], df.ix[-testingsize:]


#econdata(endyear=2002)
X_brief, y_brief, y_brief_adj, X_mkt, y_mkt, y_mkt_adj = processframe(pd.read_table('econdata.tsv'))


# randomly sort X and y dataframes together
X_brief = X_brief.reindex(np.random.permutation(X_brief.index))
y_brief = y_brief.reindex(X_brief.index)
y_brief_adj = y_brief_adj.reindex(X_brief.index)

X_mkt = X_mkt.reindex(np.random.permutation(X_mkt.index))
y_mkt = y_mkt.reindex(X_mkt.index)
y_mkt_adj = y_mkt_adj.reindex(X_mkt.index)


# break into training, cross validation, and testing data sets
X_brief_train, X_brief_cv, X_brief_test = training_validation_testing_sets(X_brief)
y_brief_train, y_brief_cv, y_brief_test = training_validation_testing_sets(y_brief)
y_brief_adj_train, y_brief_adj_cv, y_brief_adj_test = training_validation_testing_sets(y_brief_adj)

X_mkt_train, X_mkt_cv, X_mkt_test = training_validation_testing_sets(X_mkt)
y_mkt_train, y_mkt_cv, y_mkt_test = training_validation_testing_sets(y_mkt)
y_mkt_adj_train, y_mkt_adj_cv, y_mkt_adj_test = training_validation_testing_sets(y_mkt_adj)



#for item in X_brief_train.columns:
#    print(item)
'''
NEXT STEPS:
-scikit learn and perform linear regression with regularization
'''

# Create linear regression object
regr = Ridge(alpha=100)

# Train the model using the training sets

# drop errors
errors = y_brief_train['r_Open_after'][y_brief_train['r_Open_after'].apply(lambda x: np.isnan(x))]
X_brief_train = X_brief_train.drop(errors.index)
y_brief_train = y_brief_train.drop(errors.index)
y_brief_adj_train = y_brief_adj_train.drop(errors.index)

errors = y_brief_cv['r_Open_after'][y_brief_cv['r_Open_after'].apply(lambda x: np.isnan(x))]
X_brief_cv = X_brief_cv.drop(errors.index)
y_brief_cv = y_brief_cv.drop(errors.index)
y_brief_adj_cv = y_brief_adj_cv.drop(errors.index)

errors = y_brief_test['r_Open_after'][y_brief_test['r_Open_after'].apply(lambda x: np.isnan(x))]
X_brief_test = X_brief_test.drop(errors.index)
y_brief_test = y_brief_test.drop(errors.index)
y_brief_adj_test = y_brief_adj_test.drop(errors.index)

#assert not np.any(np.isnan(X_brief_train) | np.isinf(X_brief_train))
#assert not np.any(np.isnan(y_brief_train['r_Open_after']))
#assert not np.any(np.isinf(y_brief_train['r_Open_after']))

regr.fit(X_brief_train.values, y_brief_train['r_Open_after'].values)

# The coefficients
print('Coefficients: \n', regr.coef_)
for item in regr.coef_:
    print(item)

# The mean square error
#print("Residual sum of squares: %.2f" % np.mean((regr.predict(X_brief_cv.values) - y_brief_cv['r_Open_after'].values) ** 2))
print("Residual sum of squares: %s" % np.mean((regr.predict(X_brief_cv.values) - y_brief_cv['r_Open_after'].values) ** 2))

# Explained variance score: 1 is perfect prediction
print('Variance score: %s' % regr.score(X_brief_cv.values, y_brief_cv['r_Open_after'].values))