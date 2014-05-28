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
    '''
    K ==> * 1000
    M ==> * 1000000
    B ==> * 1000000000
    % ==> drop %, divide by 100
    $ ==> remove $
    --- ==> NA
    '''
    def processelement(element):
        if not isinstance(element, str) or ':' in element:
            return element
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
    return econdf.applymap(processelement)

#econdata(endyear=2002)
#print(processframe(pd.read_table('econdata.tsv')))