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
        if not isinstance(element, str) or ':' in element:
            return str(element)
        if element[0] == '$' or element[1] == '$':
            element = element.replace('$','')
        if element[-1] == 'K':
            return float(element[:-1]) * 1000
        elif element[-1] == 'M':
            return float(element[:-1]) * 1000000
        elif element[-1] == 'B':
            return float(element[:-1]) * 1000000000
        elif element[-1] == '%':
            return float(element[:-1]) / 100
        elif element == '---':
            return NA
        else:
            return element

    def getclosebeforedate(dfrow):
        time = dfrow['Time (ET)']
        s = datetime.strptime(dfrow['Date'] + ' ' + dfrow['Year'], '%b %d %Y')
        if time[-2:] == 'AM': s = s - BDay(1) # close should refer to yesterday's date otherwise refer to date of event
        return s

    def getopenafterdate(dfrow):
        return dfrow['close date before event'] + BDay(1)

    df = econdf.applymap(processelement)
    df['close date before event'] = df.apply(getclosebeforedate, axis=1)
    df['open date after event'] = df.apply(getopenafterdate, axis=1)
    merged = pd.merge(df, mktdata(), left_on='close date before event', right_index=True)
    merged.columns = ['Year', 'Week', 'Date', 'Time (ET)', 'Statistic', 'For', 'Actual', 'Briefing Forecast', 'Market Expects', 'Prior', 'Revised', 'close date before event', 'open date after event', 'Open_before', 'High_before', 'Low_before', 'Close_before', 'Volume_before', 'Adj Close_before']
    merged = pd.merge(merged, mktdata(), left_on='open date after event', right_index=True)
    merged.columns = ['Year', 'Week', 'Date', 'Time (ET)', 'Statistic', 'For', 'Actual', 'Briefing Forecast', 'Market Expects', 'Prior', 'Revised', 'close date before event', 'open date after event', 'Open_before', 'High_before', 'Low_before', 'Close_before', 'Volume_before', 'Adj Close_before', 'Open_after', 'High_after', 'Low_after', 'Close_after', 'Volume_after', 'Adj Close_after']    
    return merged


#econdata(endyear=2002)
df = processframe(pd.read_table('econdata.tsv'))
print(df)