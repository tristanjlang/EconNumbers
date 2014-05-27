'''
Yahoo Finance puts economic data in html at: http://biz.yahoo.com/c/ec/200101.html
URL format is: "<four digit year><week of year>.html"

It is in an html table, so I should be able to just extract the data from the html.
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
'''

from bs4 import BeautifulSoup
from urllib import request

r1 = request.urlopen('http://biz.yahoo.com/c/ec/200101.html')
r2 = request.urlopen('http://biz.yahoo.com/c/ec/200102.html')
r14 = request.urlopen('http://biz.yahoo.com/c/ec/201402.html')

soup1 = BeautifulSoup(r1.readall())
soup2 = BeautifulSoup(r2.readall())
soup14 = BeautifulSoup(r14.readall())

